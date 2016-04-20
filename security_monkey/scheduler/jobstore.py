import datetime

import pytz
from apscheduler.job import Job
from apscheduler.jobstores.base import BaseJobStore
from apscheduler.triggers.interval import IntervalTrigger

from security_monkey import app, db
from security_monkey.datastore import Account

TIME_DELAY = 15


class SecurityMonkeyJobStore(BaseJobStore):
    def __init__(self, job_func, *args, **kwargs):
        super(SecurityMonkeyJobStore, self).__init__(*args, **kwargs)
        self.job_func = job_func

    def lookup_job(self, job_id):
        """
        Returns a specific job, or ``None`` if it isn't found..

        The job store is responsible for setting the ``scheduler`` and ``jobstore`` attributes of the returned job to
        point to the scheduler and itself, respectively.

        :param str|unicode job_id: identifier of the job
        :rtype: Job
        """
        app.logger.debug("Lookup job {}".format(job_id))
        raise NotImplementedError()

    def get_all_jobs(self):
        """
        Returns a list of all jobs in this job store. The returned jobs should be sorted by next run time (ascending).
        Paused jobs (next_run_time == None) should be sorted last.

        The job store is responsible for setting the ``scheduler`` and ``jobstore`` attributes of the returned jobs to
        point to the scheduler and itself, respectively.

        :rtype: list[Job]
        """

        app.logger.debug("Getting all jobs")
        raise NotImplementedError()

    def get_due_jobs(self, now):
        """
        Returns generatpr of jobs that have ``next_run_time`` earlier or equal to ``now``.
        The returned jobs must be sorted by next run time (ascending).

        :param datetime.datetime now: the current (timezone aware) datetime
        :rtype: generator[Job]
        """

        app.logger.debug("Getting due jobs")

        accounts = Account.query\
            .filter_by(active=True)\
            .filter(Account.sched_last_run < (datetime.datetime.utcnow() - datetime.timedelta(minutes=15)))

        account = accounts.first()

        if not account:
            accounts = Account.query\
                .filter_by(active=True)\
                .filter(Account.sched_last_run == None)
            account = accounts.first()

        app.logger.debug("Got job: {}".format(account))

        while account:
            # Needs to be TZ aware for sqlalchemy
            account.sched_last_run = now.replace(tzinfo=pytz.utc)
            db.session.add(account)
            db.session.commit()

            job = Job.__new__(Job)
            job.name = account.name
            job.id = str(account.id)
            job.func = self.job_func
            # job.kwargs = {"interval": account.sched_interval}
            job.kwargs = {}
            job.args = (account.name,)
            job.executor = "default"
            job.coalesce = True
            job.next_run_time = now
            job.trigger = IntervalTrigger(minutes=TIME_DELAY)
            job.max_instances = 1
            job.misfire_grace_time = 30
            job._scheduler = self._scheduler
            job._jobstore_alias = self._alias

            yield job
            # Need to create new query every time, calling first() execute the query
            accounts = Account.query\
                .filter_by(active=True)\
                .filter((Account.sched_last_run + datetime.timedelta(minutes=TIME_DELAY)) < now)
            account = accounts.first()

    def get_next_run_time(self):
        """
        Returns the earliest run time of all the jobs stored in this job store, or ``None`` if there are no active jobs.

        :rtype: datetime.datetime
        """

        app.logger.debug("Getting next runtime")

        account = Account.query\
            .filter_by(active=True)\
            .order_by(Account.sched_last_run.desc()).first()

        if not account.sched_last_run:
            val = datetime.datetime.min
            val = pytz.utc.localize(val)
        else:
            val = account.sched_last_run + datetime.timedelta(minutes=TIME_DELAY)
        app.logger.debug("Got next time {}".format(val))

        return val

    def add_job(self, job):
        """ Can't add new jobs from here! """
        raise NotImplementedError("Can't add new jobs from here!")

    def update_job(self, job):
        """
        Can't update next_run_time on db object because it has only just started
        Keep scheduler happy by returning
        """
        return

    def remove_job(self, job_id):
        # TODO flag as error on object
        """ Flag job as not runnable """
        raise NotImplementedError()

    def remove_all_jobs(self):
        # TODO flag as error on object
        """ Flag job as not runnable """
        raise NotImplementedError()
