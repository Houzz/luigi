# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
The system for scheduling tasks and executing them in order.
Deals with dependencies, priorities, resources, etc.
The :py:class:`~luigi.worker.Worker` pulls tasks from the scheduler (usually over the REST interface) and executes them.
See :doc:`/central_scheduler` for more info.
"""

import collections

try:
    import cPickle as pickle
except ImportError:
    import pickle
import datetime
import functools
import itertools
import logging
import os
import re
import time

from luigi import six

from luigi import configuration
from luigi import notifications
from luigi import parameter
from luigi import task_history as history
from luigi.task_status import BATCH_RUNNING, DISABLED, DONE, FAILED, PENDING, RUNNING, SUSPENDED, UNKNOWN
from luigi.task import Config, task_id_str

logger = logging.getLogger("luigi.server")


class Scheduler(object):
    """
    Abstract base class.

    Note that the methods all take string arguments, not Task objects...
    """""
    add_task = NotImplemented
    get_work = NotImplemented
    ping = NotImplemented

UPSTREAM_RUNNING = 'UPSTREAM_RUNNING'
UPSTREAM_MISSING_INPUT = 'UPSTREAM_MISSING_INPUT'
UPSTREAM_FAILED = 'UPSTREAM_FAILED'
UPSTREAM_DISABLED = 'UPSTREAM_DISABLED'

UPSTREAM_SEVERITY_ORDER = (
    '',
    UPSTREAM_RUNNING,
    UPSTREAM_MISSING_INPUT,
    UPSTREAM_FAILED,
    UPSTREAM_DISABLED,
)
UPSTREAM_SEVERITY_KEY = UPSTREAM_SEVERITY_ORDER.index
STATUS_TO_UPSTREAM_MAP = {
    FAILED: UPSTREAM_FAILED,
    RUNNING: UPSTREAM_RUNNING,
    BATCH_RUNNING: UPSTREAM_RUNNING,
    PENDING: UPSTREAM_MISSING_INPUT,
    DISABLED: UPSTREAM_DISABLED,
}

TASKS = 'tasks'
ACTIVE_WORKERS = 'active_workers'
BATCH_TASKS = 'batch_tasks'
RUNNING_BATCHES = 'running_batches'

TASK_FAMILY_RE = re.compile(r'([^(_]+)[(_]')

AGGREGATE_FUNCTIONS = {
    'csv': ','.join,
    'max': max,
    'min': min,
    'range': lambda args: '%s-%s' % (min(args), max(args)),
}


class scheduler(Config):
    # TODO(erikbern): the config_path is needed for backwards compatilibity. We should drop the compatibility
    # at some point (in particular this would force users to replace all dashes with underscores in the config)
    retry_delay = parameter.FloatParameter(default=900.0)
    remove_delay = parameter.FloatParameter(default=600.0)
    worker_disconnect_delay = parameter.FloatParameter(default=60.0)
    state_path = parameter.Parameter(default='/var/lib/luigi-server/state.pickle')

    # Jobs are disabled if we see more than disable_failures failures in disable_window seconds.
    # These disables last for disable_persist seconds.
    disable_window = parameter.IntParameter(default=3600,
                                            config_path=dict(section='scheduler', name='disable-window-seconds'))
    disable_failures = parameter.IntParameter(default=None,
                                              config_path=dict(section='scheduler', name='disable-num-failures'))
    disable_hard_timeout = parameter.IntParameter(default=None,
                                                  config_path=dict(section='scheduler', name='disable-hard-timeout'))
    disable_persist = parameter.IntParameter(default=86400,
                                             config_path=dict(section='scheduler', name='disable-persist-seconds'))
    max_shown_tasks = parameter.IntParameter(default=100000)
    max_graph_nodes = parameter.IntParameter(default=100000)
    prune_done_tasks = parameter.BoolParameter(default=False)

    record_task_history = parameter.BoolParameter(default=False)

    prune_on_get_work = parameter.BoolParameter(default=False)
    prefer_newer_tasks = parameter.BoolParameter(default=False)


def fix_time(x):
    # Backwards compatibility for a fix in Dec 2014. Prior to the fix, pickled state might store datetime objects
    # Let's remove this function soon
    if isinstance(x, datetime.datetime):
        return time.mktime(x.timetuple())
    else:
        return x


class Failures(object):
    """
    This class tracks the number of failures in a given time window.

    Failures added are marked with the current timestamp, and this class counts
    the number of failures in a sliding time window ending at the present.
    """

    def __init__(self, window):
        """
        Initialize with the given window.

        :param window: how long to track failures for, as a float (number of seconds).
        """
        self.window = window
        self.failures = collections.deque()
        self.first_failure_time = None

    def add_failure(self):
        """
        Add a failure event with the current timestamp.
        """
        failure_time = time.time()

        if not self.first_failure_time:
            self.first_failure_time = failure_time

        self.failures.append(failure_time)

    def num_failures(self):
        """
        Return the number of failures in the window.
        """
        min_time = time.time() - self.window

        while self.failures and fix_time(self.failures[0]) < min_time:
            self.failures.popleft()

        return len(self.failures)

    def clear(self):
        """
        Clear the failure queue.
        """
        self.failures.clear()


def _get_default(x, default):
    if x is not None:
        return x
    else:
        return default


def rank_prefer_older(task):
    return task.priority, -task.time


def rank_prefer_newer(task):
    return task.priority, task.time


class Task(object):

    def __init__(self, task_id, status, deps, resources=None, priority=0, family='', module=None,
                 params=None, disable_failures=None, disable_window=None, disable_hard_timeout=None,
                 tracking_url=None, is_batch=False):
        self.id = task_id
        self.stakeholders = set()  # workers ids that are somehow related to this task (i.e. don't prune while any of these workers are still active)
        self.workers = set()  # workers ids that can perform task - task is 'BROKEN' if none of these workers are active
        if deps is None:
            self.deps = set()
        else:
            self.deps = set(deps)
        self.status = status  # PENDING, RUNNING, FAILED or DONE
        self.time = time.time()  # Timestamp when task was first added
        self.updated = self.time
        self.retry = None
        self.remove = None
        self.worker_running = None  # the worker id that is currently running the task or None
        self.time_running = None  # Timestamp when picked up by worker
        self.expl = None
        self.priority = priority
        self.resources = _get_default(resources, {})
        self.family = family
        self.module = module
        self.params = _get_default(params, {})
        self.disable_failures = disable_failures
        self.disable_hard_timeout = disable_hard_timeout
        self.failures = Failures(disable_window)
        self.tracking_url = tracking_url
        self.scheduler_disable_time = None
        self.runnable = False
        self.is_batch = is_batch
        self.batchable = False

    def __repr__(self):
        return "Task(%r)" % vars(self)

    def add_failure(self):
        self.failures.add_failure()

    def has_excessive_failures(self):
        if (self.failures.first_failure_time is not None and
                self.disable_hard_timeout):
            if (time.time() >= self.failures.first_failure_time +
                    self.disable_hard_timeout):
                return True

        if self.failures.num_failures() >= self.disable_failures:
            return True

        return False

    def can_disable(self):
        return (self.disable_failures is not None or
                self.disable_hard_timeout is not None)

    @property
    def pretty_id(self):
        param_str = ', '.join('{}={}'.format(key, value) for key, value in self.params.items())
        return '{}({})'.format(self.family, param_str)


class TaskBatcher(object):
    def __init__(self, family, args, aggregate_args, max_batch_size):
        self.family = family
        self.args = args
        self.aggregates = aggregate_args
        self.max_batch_size = max_batch_size

    def _valid_batch_size(self, tasks):
        if self.max_batch_size is None:
            return True
        elif len(tasks) > self.max_batch_size:
            return False
        elif 'range' in self.aggregates.values():
            for arg, agg_type in self.aggregates.items():
                if agg_type == 'range':
                    params = [task.params[arg] for task in tasks]
                    low_bound, up_bound = min(params), max(params)
                    try:
                        low_bound = datetime.datetime.strptime(low_bound, '%Y-%m-%d')
                        up_bound = datetime.datetime.strptime(up_bound, '%Y-%m-%d')
                    except ValueError:
                        continue
                    diff = (up_bound - low_bound).days + 1
                    if diff > self.max_batch_size:
                        return False
        return True

    def task_id(self, tasks):
        if not self._valid_batch_size(tasks):
            return None, None
        params = {}
        for task_arg, batch_arg in self.args:
            raw_vals = [task.params[task_arg] for task in tasks]
            agg_function = self.aggregates.get(task_arg)
            if agg_function:
                arg_val = AGGREGATE_FUNCTIONS[agg_function](raw_vals)
            elif any(v != raw_vals[0] for v in raw_vals):
                return None, None
            else:
                arg_val = raw_vals[0]
            params[batch_arg] = arg_val
        return task_id_str(self.family, params), params


class Worker(object):
    """
    Structure for tracking worker activity and keeping their references.
    """

    def __init__(self, worker_id, last_active=None):
        self.id = worker_id
        self.reference = None  # reference to the worker in the real world. (Currently a dict containing just the host)
        self.last_active = last_active or time.time()  # seconds since epoch
        self.last_get_work = None
        self.started = time.time()  # seconds since epoch
        self.tasks = set()  # task objects
        self.info = {}
        self.disabled = False

    def add_info(self, info):
        self.info.update(info)

    def update(self, worker_reference, get_work=False):
        if worker_reference:
            self.reference = worker_reference
        self.last_active = time.time()
        if get_work:
            self.last_get_work = time.time()

    def prune(self, config):
        # Delete workers that haven't said anything for a while (probably killed)
        if self.last_active + config.worker_disconnect_delay < time.time():
            return True

    def get_pending_tasks(self, state):
        """
        Get PENDING (and RUNNING) tasks for this worker.

        You have to pass in the state for optimization reasons.
        """
        if len(self.tasks) < state.num_pending_tasks():
            return six.moves.filter(lambda task: task.status in [PENDING, RUNNING],
                                    self.tasks)
        else:
            return state.get_pending_tasks()

    def is_trivial_worker(self, state):
        """
        If it's not an assistant having only tasks that are without
        requirements.

        We have to pass the state parameter for optimization reasons.
        """
        if self.assistant:
            return False
        return all(not task.resources for task in self.get_pending_tasks(state))

    @property
    def assistant(self):
        return self.info.get('assistant', False)

    def __str__(self):
        return self.id


class SimpleTaskState(object):
    """
    Keep track of the current state and handle persistance.

    The point of this class is to enable other ways to keep state, eg. by using a database
    These will be implemented by creating an abstract base class that this and other classes
    inherit from.
    """

    def __init__(self, state_path):
        self._state_path = state_path
        self._tasks = {}  # map from id to a Task object
        self._batch_tasks = {}  # map from family to TaskBatch object
        self._running_batches = {}  # map from id to list of batched task ids
        self._status_tasks = collections.defaultdict(dict)
        self._active_workers = {}  # map from id to a Worker object

    def get_state(self):
        return {
            TASKS: self._tasks,
            ACTIVE_WORKERS: self._active_workers,
            BATCH_TASKS: self._batch_tasks,
            RUNNING_BATCHES: self._running_batches,
        }

    def set_state(self, state):
        if isinstance(state, dict):
            self._tasks = state.get(TASKS, {})
            self._active_workers = state.get(ACTIVE_WORKERS, {})
            self._batch_tasks = state.get(BATCH_TASKS, {})
            self._running_batches = state.get(RUNNING_BATCHES, {})
        else:
            self._tasks, self._active_workers = state
            self._batch_tasks = {}

    def dump(self):
        try:
            with open(self._state_path, 'wb') as fobj:
                pickle.dump(self.get_state(), fobj)
        except IOError:
            logger.warning("Failed saving scheduler state", exc_info=1)
        else:
            logger.info("Saved state in %s", self._state_path)

    # prone to lead to crashes when old state is unpickled with updated code. TODO some kind of version control?
    def load(self):
        if os.path.exists(self._state_path):
            logger.info("Attempting to load state from %s", self._state_path)
            try:
                with open(self._state_path, 'rb') as fobj:
                    state = pickle.load(fobj)
            except BaseException:
                logger.exception("Error when loading state. Starting from clean slate.")
                return

            self.set_state(state)
            self._status_tasks = collections.defaultdict(dict)
            for task in six.itervalues(self._tasks):
                self._status_tasks[task.status][task.id] = task

            # Convert from old format
            # TODO: this is really ugly, we need something more future-proof
            # Every time we add an attribute to the Worker or Task class, this
            # code needs to be updated

            # Compatibility since 2014-06-02
            for k, v in six.iteritems(self._active_workers):
                if isinstance(v, float):
                    self._active_workers[k] = Worker(worker_id=k, last_active=v)

            # Compatibility since 2015-05-28
            if any(not hasattr(w, 'tasks') for k, w in six.iteritems(self._active_workers)):
                # If you load from an old format where Workers don't contain tasks.
                for k, worker in six.iteritems(self._active_workers):
                    worker.tasks = set()
                for task in six.itervalues(self._tasks):
                    for worker_id in task.workers:
                        self._active_workers[worker_id].tasks.add(task)

            # Compatibility since 2015-04-28
            if any(not hasattr(t, 'disable_hard_timeout') for t in six.itervalues(self._tasks)):
                for t in six.itervalues(self._tasks):
                    t.disable_hard_timeout = None

            # Compatibility since 2016-02-03
            if any(not hasattr(t, 'is_batch') for t in six.itervalues(self._tasks)):
                for t in six.itervalues(self._tasks):
                    t.batchable = False
                    t.is_batch = False

        else:
            logger.info("No prior state file exists at %s. Starting with clean slate", self._state_path)

    def get_active_tasks(self, status=None):
        if status:
            for task in six.itervalues(self._status_tasks[status]):
                yield task
        else:
            for task in six.itervalues(self._tasks):
                yield task

    def get_running_tasks(self):
        return six.itervalues(self._status_tasks[RUNNING])

    def get_pending_tasks(self):
        return itertools.chain.from_iterable(six.itervalues(self._status_tasks[status])
                                             for status in [PENDING, RUNNING])

    def get_batch(self, worker_id, tasks):
        if len(tasks) == 1:
            return tasks[0]
        families = set(task.family for task in tasks)
        if len(families) != 1:
            return None
        family = families.pop()
        batch_task = self.get_batcher(worker_id, family)
        if batch_task is None:
            return None
        task_id, params = batch_task.task_id(tasks)
        if task_id is None:
            return None
        priority = max(task.priority for task in tasks)
        resource_keys = reduce(set.union, (task.resources.keys() for task in tasks), set())
        resources = {key: max(task.resources.get(key, 0) for task in tasks) for key in resource_keys}
        deps = reduce(set.union, (task.deps for task in tasks), set())
        batch_task_obj = Task(
            task_id=task_id,
            status=PENDING,
            deps=deps,
            priority=priority,
            resources=resources,
            family=batch_task.family,
            params=params,
            is_batch=True,
        )
        batch_task_obj.stakeholders.add(worker_id)
        batch_task_obj.workers.add(worker_id)
        return batch_task_obj

    def num_pending_tasks(self):
        """
        Return how many tasks are PENDING + RUNNING. O(1).
        """
        return len(self._status_tasks[PENDING]) + len(self._status_tasks[RUNNING])

    def get_task(self, task_id, default=None, setdefault=None):
        if setdefault:
            task = self._tasks.setdefault(task_id, setdefault)
            self._status_tasks[task.status][task.id] = task
            return task
        else:
            return self._tasks.get(task_id, default)

    def get_batcher(self, worker, family):
        return self._batch_tasks.get((worker, family))

    def set_batcher(self, worker, family, batcher_family, batcher_args, batcher_aggregate_args, max_batch_size):
        batcher = TaskBatcher(batcher_family, batcher_args, batcher_aggregate_args, max_batch_size)
        self._batch_tasks[(worker, family)] = batcher
        return batcher

    def has_task(self, task_id):
        return task_id in self._tasks

    def re_enable(self, task, config=None):
        task.scheduler_disable_time = None
        task.failures.clear()
        if config:
            self.set_status(task, FAILED, config)
            task.failures.clear()

    def set_status(self, task, new_status, config=None, batch=None):
        if new_status == FAILED:
            assert config is not None

        if new_status == DISABLED and task.status in (RUNNING, BATCH_RUNNING):
            return

        if task.status == DISABLED:
            if new_status == DONE:
                self.re_enable(task)

            # don't allow workers to override a scheduler disable
            elif task.scheduler_disable_time is not None and new_status != DISABLED:
                return

        if new_status == FAILED and task.can_disable() and task.status != DISABLED and not task.is_batch:
            task.add_failure()
            if task.has_excessive_failures():
                task.scheduler_disable_time = time.time()
                new_status = DISABLED
                notifications.send_error_email(
                    'Luigi Scheduler: DISABLED {task} due to excessive failures'.format(task=task.id),
                    '{task} failed {failures} times in the last {window} seconds, so it is being '
                    'disabled for {persist} seconds'.format(
                        failures=config.disable_failures,
                        task=task.id,
                        window=config.disable_window,
                        persist=config.disable_persist,
                    ))
        elif new_status == DISABLED:
            task.scheduler_disable_time = None

        if new_status == RUNNING and batch:
            self._running_batches[task.id] = set(batch)
        elif task.id in self._running_batches and new_status != RUNNING:
            subtask_status = FAILED if new_status == DISABLED else new_status
            for subtask_id in self._running_batches.pop(task.id):
                subtask = self.get_task(subtask_id)
                if subtask_status == FAILED:
                    subtask.retry = time.time() + config.retry_delay
                self.set_status(subtask, subtask_status, config)

        if new_status != task.status:
            self._status_tasks[task.status].pop(task.id)
            self._status_tasks[new_status][task.id] = task
            task.status = new_status
            task.updated = time.time()

    def fail_dead_worker_task(self, task, config, assistants):
        # If a running worker disconnects, tag all its jobs as FAILED and subject it to the same retry logic
        if task.status in (RUNNING, BATCH_RUNNING) and task.worker_running and task.worker_running not in task.stakeholders | assistants:
            logger.info("Task %r is marked as running by disconnected worker %r -> marking as "
                        "FAILED with retry delay of %rs", task.id, task.worker_running,
                        config.retry_delay)
            task.worker_running = None
            self.set_status(task, FAILED, config)
            task.retry = time.time() + config.retry_delay

    def prune(self, task, config):
        remove = False

        # Mark tasks with no remaining active stakeholders for deletion
        if not task.stakeholders:
            if task.remove is None:
                logger.info("Task %r has stakeholders %r but none remain connected -> will remove "
                            "task in %s seconds", task.id, task.stakeholders, config.remove_delay)
                task.remove = time.time() + config.remove_delay

        # Re-enable task after the disable time expires
        if task.status == DISABLED and task.scheduler_disable_time is not None:
            if time.time() - fix_time(task.scheduler_disable_time) > config.disable_persist:
                self.re_enable(task, config)

        # Remove tasks that have no stakeholders
        if task.remove and time.time() > task.remove:
            logger.info("Removing task %r (no connected stakeholders)", task.id)
            remove = True

        if task.is_batch and task.status != RUNNING:
            logger.info("Removing batch task %r", task.id)
            remove = True

        # Reset FAILED tasks to PENDING if max timeout is reached, and retry delay is >= 0
        if task.status == FAILED and config.retry_delay >= 0 and task.retry < time.time():
            self.set_status(task, PENDING, config)

        return remove

    def inactivate_tasks(self, delete_tasks):
        # The terminology is a bit confusing: we used to "delete" tasks when they became inactive,
        # but with a pluggable state storage, you might very well want to keep some history of
        # older tasks as well. That's why we call it "inactivate" (as in the verb)
        for task in delete_tasks:
            task_obj = self._tasks.pop(task)
            self._status_tasks[task_obj.status].pop(task)

    def get_active_workers(self, last_active_lt=None, last_get_work_gt=None):
        for worker in six.itervalues(self._active_workers):
            if last_active_lt is not None and worker.last_active >= last_active_lt:
                continue
            last_get_work = getattr(worker, 'last_get_work', None)
            if last_get_work_gt is not None and (
                    last_get_work is None or last_get_work <= last_get_work_gt):
                continue
            yield worker

    def get_assistants(self, last_active_lt=None):
        return filter(lambda w: w.assistant, self.get_active_workers(last_active_lt))

    def get_worker_ids(self):
        return self._active_workers.keys()  # only used for unit tests

    def get_worker(self, worker_id):
        return self._active_workers.setdefault(worker_id, Worker(worker_id))

    def inactivate_workers(self, delete_workers):
        # Mark workers as inactive
        for worker in delete_workers:
            self._active_workers.pop(worker)
        self._remove_workers_from_tasks(delete_workers)

    def _remove_workers_from_tasks(self, workers, remove_stakeholders=True):
        for task in self.get_active_tasks():
            if remove_stakeholders:
                task.stakeholders.difference_update(workers)
            task.workers.difference_update(workers)

    def disable_workers(self, workers):
        self._remove_workers_from_tasks(workers, remove_stakeholders=False)
        for worker in workers:
            self.get_worker(worker).disabled = True

    def get_necessary_tasks(self):
        necessary_tasks = set()
        for task in self.get_active_tasks():
            if task.status not in (DONE, DISABLED) or \
                    getattr(task, 'scheduler_disable_time', None) is not None:
                necessary_tasks.update(task.deps)
                necessary_tasks.add(task.id)
        return necessary_tasks

    def update_tracking_url(self, task, tracking_url):
        task.tracking_url = tracking_url
        for batch_task_id in self._running_batches.get(task.id, []):
            batch_task = self.get_task(batch_task_id)
            if batch_task:
                batch_task.tracking_url = tracking_url


class CentralPlannerScheduler(Scheduler):
    """
    Async scheduler that can handle multiple workers, etc.

    Can be run locally or on a server (using RemoteScheduler + server.Server).
    """

    def __init__(self, config=None, resources=None, task_history_impl=None, **kwargs):
        """
        Keyword Arguments:
        :param config: an object of class "scheduler" or None (in which the global instance will be used)
        :param resources: a dict of str->int constraints
        :param task_history_override: ignore config and use this object as the task history
        """
        self._config = config or scheduler(**kwargs)
        self._state = SimpleTaskState(self._config.state_path)

        if task_history_impl:
            self._task_history = task_history_impl
        elif self._config.record_task_history:
            from luigi import db_task_history  # Needs sqlalchemy, thus imported here
            self._task_history = db_task_history.DbTaskHistory()
        else:
            self._task_history = history.NopHistory()
        self._resources = resources or configuration.get_config().getintdict('resources')  # TODO: Can we make this a Parameter?
        self._make_task = functools.partial(
            Task, disable_failures=self._config.disable_failures,
            disable_hard_timeout=self._config.disable_hard_timeout,
            disable_window=self._config.disable_window)
        self._worker_requests = {}
        self._rank = rank_prefer_newer if self._config.prefer_newer_tasks else rank_prefer_older

    def load(self):
        self._state.load()

    def dump(self):
        self._state.dump()

    def prune(self):
        logger.info("Starting pruning of task graph")
        remove_workers = []
        for worker in self._state.get_active_workers():
            if worker.prune(self._config):
                logger.info("Worker %s timed out (no contact for >=%ss)", worker, self._config.worker_disconnect_delay)
                remove_workers.append(worker.id)

        self._state.inactivate_workers(remove_workers)

        assistant_ids = set(w.id for w in self._state.get_assistants())
        remove_tasks = []

        if assistant_ids:
            necessary_tasks = self._state.get_necessary_tasks()
        else:
            necessary_tasks = ()

        for task in self._state.get_active_tasks():
            self._state.fail_dead_worker_task(task, self._config, assistant_ids)
            removed = self._state.prune(task, self._config)
            if removed and task.id not in necessary_tasks:
                remove_tasks.append(task.id)

        self._state.inactivate_tasks(remove_tasks)

        logger.info("Done pruning task graph")

    def update(self, worker_id, worker_reference=None, get_work=False):
        """
        Keep track of whenever the worker was last active.
        """
        worker = self._state.get_worker(worker_id)
        worker.update(worker_reference, get_work=get_work)
        return not getattr(worker, 'disabled', False)

    def _update_priority(self, task, prio, worker):
        """
        Update priority of the given task.

        Priority can only be increased.
        If the task doesn't exist, a placeholder task is created to preserve priority when the task is later scheduled.
        """
        task.priority = prio = max(prio, task.priority)
        for dep in task.deps or []:
            t = self._state.get_task(dep)
            if t is not None and prio > t.priority:
                self._update_priority(t, prio, worker)

    def add_task_batcher(self, worker, family, batcher_family, batcher_args, batcher_aggregate_args, max_batch_size=None):
        self._state.set_batcher(worker, family, batcher_family, batcher_args, batcher_aggregate_args, max_batch_size)

    def add_task(self, task_id=None, status=PENDING, runnable=True,
                 deps=None, new_deps=None, expl=None, resources=None,
                 priority=0, family='', module=None, params=None,
                 assistant=False, tracking_url=None, batchable=None,
                 **kwargs):
        """
        * add task identified by task_id if it doesn't exist
        * if deps is not None, update dependency list
        * update status of task
        * add additional workers/stakeholders
        * update priority when needed
        """
        worker_id = kwargs['worker']
        worker_enabled = self.update(worker_id)

        if worker_enabled:
            _default_task = self._make_task(
                task_id=task_id, status=PENDING, deps=deps, resources=resources,
                priority=priority, family=family, module=module, params=params,
            )
        else:
            _default_task = None

        task = self._state.get_task(task_id, setdefault=_default_task)

        if task is None or (task.status != RUNNING and not worker_enabled):
            return

        # for setting priority, we'll sometimes create tasks with unset family and params
        if not task.family:
            task.family = family
        if not getattr(task, 'module', None):
            task.module = module
        if params or not task.params:
            task.params = _get_default(params, {})

        if tracking_url is not None or task.status != RUNNING:
            self._state.update_tracking_url(task, tracking_url)

        if task.remove is not None:
            task.remove = None  # unmark task for removal so it isn't removed after being added

        if expl is not None:
            task.expl = expl

        if batchable is not None:
            task.batchable = batchable

        if not (task.status in (RUNNING, BATCH_RUNNING) and status == PENDING) or new_deps:
            # don't allow re-scheduling of task while it is running, it must either fail or succeed first
            if status == PENDING or status != task.status:
                # Update the DB only if there was a acctual change, to prevent noise.
                # We also check for status == PENDING b/c that's the default value
                # (so checking for status != task.status woule lie)
                self._update_task_history(task, status)
            self._state.set_status(task, PENDING if status == SUSPENDED else status, self._config)
            if status == FAILED:
                task.retry = self._retry_time(task, self._config)

        if deps is not None:
            task.deps = set(deps)

        if new_deps is not None:
            task.deps.update(new_deps)

        if resources is not None:
            task.resources = resources

        if worker_enabled and not assistant:
            task.stakeholders.add(worker_id)

            # Task dependencies might not exist yet. Let's create dummy tasks for them for now.
            # Otherwise the task dependencies might end up being pruned if scheduling takes a long time
            for dep in task.deps or []:
                t = self._state.get_task(dep, setdefault=self._make_task(task_id=dep, status=UNKNOWN, deps=None, priority=priority))
                t.stakeholders.add(worker_id)

        self._update_priority(task, priority, worker_id)

        if runnable and status != FAILED and worker_enabled:
            task.workers.add(worker_id)
            self._state.get_worker(worker_id).tasks.add(task)
            task.runnable = runnable

    def add_worker(self, worker, info, **kwargs):
        self._state.get_worker(worker).add_info(info)

    def disable_worker(self, worker):
        self._state.disable_workers({worker})

    def update_resources(self, resources=None):
        if resources is None:
            config = configuration.get_config()
            config.reload()
            resources = config.getintdict('resources')
        self._resources = resources

    def _has_resources(self, needed_resources, used_resources):
        if needed_resources is None:
            return True

        available_resources = self._resources or {}
        for resource, amount in six.iteritems(needed_resources):
            if amount + used_resources[resource] > available_resources.get(resource, 1):
                return False
        return True

    def _used_resources(self):
        used_resources = collections.defaultdict(int)
        if self._resources is not None:
            for task in self._state.get_running_tasks():
                if task.status == RUNNING and task.resources:
                    for resource, amount in six.iteritems(task.resources):
                        used_resources[resource] += amount
        return used_resources

    def _schedulable(self, task):
        if task.status != PENDING:
            return False
        for dep in task.deps:
            dep_task = self._state.get_task(dep, default=None)
            if dep_task is None or dep_task.status != DONE:
                return False
        return True

    def _retry_time(self, task, config):
        return time.time() + config.retry_delay

    def get_work(self, host=None, assistant=False, current_tasks=None, **kwargs):
        # TODO: remove any expired nodes

        # Algo: iterate over all nodes, find the highest priority node no dependencies and available
        # resources.

        # Resource checking looks both at currently available resources and at which resources would
        # be available if all running tasks died and we rescheduled all workers greedily. We do both
        # checks in order to prevent a worker with many low-priority tasks from starving other
        # workers with higher priority tasks that share the same resources.

        # TODO: remove tasks that can't be done, figure out if the worker has absolutely
        # nothing it can wait for

        if self._config.prune_on_get_work:
            self.prune()

        worker_id = kwargs['worker']
        # Return remaining tasks that have no FAILED descendants
        self.update(worker_id, {'host': host}, get_work=True)
        if assistant:
            self.add_worker(worker_id, [('assistant', assistant)])

        best_task = None
        if current_tasks is not None:
            ct_set = set(current_tasks)
            for task in sorted(self._state.get_running_tasks(), key=self._rank):
                if task.worker_running == worker_id and task.id not in ct_set:
                    best_task = task
        best_tasks = [] if best_task is None else [best_task]

        locally_pending_tasks = 0
        running_tasks = []
        upstream_table = {}

        greedy_resources = collections.defaultdict(int)
        n_unique_pending = 0

        worker = self._state.get_worker(worker_id)
        if worker.is_trivial_worker(self._state):
            relevant_tasks = worker.get_pending_tasks(self._state)
            used_resources = collections.defaultdict(int)
            greedy_workers = dict()  # If there's no resources, then they can grab any task
        else:
            relevant_tasks = self._state.get_pending_tasks()
            used_resources = self._used_resources()
            activity_limit = time.time() - self._config.worker_disconnect_delay
            active_workers = self._state.get_active_workers(last_get_work_gt=activity_limit)
            greedy_workers = dict((worker.id, worker.info.get('workers', 1))
                                  for worker in active_workers)
        tasks = list(relevant_tasks)
        tasks.sort(key=self._rank, reverse=True)

        for task in tasks:
            upstream_status = self._upstream_status(task.id, upstream_table)
            in_workers = (assistant and getattr(task, 'runnable', bool(task.workers))) or worker_id in task.workers
            if task.status == RUNNING and in_workers:
                # Return a list of currently running tasks to the client,
                # makes it easier to troubleshoot
                other_worker = self._state.get_worker(task.worker_running)
                more_info = {'task_id': task.id, 'worker': str(other_worker)}
                if other_worker is not None:
                    more_info.update(other_worker.info)
                    running_tasks.append(more_info)

            if task.status == PENDING and in_workers and upstream_status != UPSTREAM_DISABLED:
                locally_pending_tasks += 1
                if len(task.workers) == 1 and not assistant:
                    n_unique_pending += 1

            if (in_workers and best_tasks and task.batchable and
                    self._state.get_batch(worker_id, best_tasks + [task]) is not None and
                    self._schedulable(task) and
                    self._has_resources(task.resources, greedy_resources)):
                best_tasks.append(task)

            if best_tasks:
                continue

            greedy_schedulable = self._has_resources(task.resources, greedy_resources)
            if task.status == RUNNING and task.worker_running in greedy_workers and greedy_schedulable:
                greedy_workers[task.worker_running] -= 1
                for resource, amount in six.iteritems((task.resources or {})):
                    greedy_resources[resource] += amount

            if self._schedulable(task) and greedy_schedulable:
                if in_workers and self._has_resources(task.resources, used_resources):
                    best_tasks = [task]
                else:
                    workers = itertools.chain(task.workers, [worker_id]) if assistant else task.workers
                    for task_worker in workers:
                        if greedy_workers.get(task_worker, 0) > 0:
                            # use up a worker
                            greedy_workers[task_worker] -= 1

                            # keep track of the resources used in greedy scheduling
                            for resource, amount in six.iteritems((task.resources or {})):
                                greedy_resources[resource] += amount

                            break

        reply = {'n_pending_tasks': locally_pending_tasks,
                 'running_tasks': running_tasks,
                 'task_id': None,
                 'n_unique_pending': n_unique_pending}

        if best_tasks:
            best_batch = self._state.get_batch(worker_id, best_tasks)
            best_task = self._state.get_task(best_batch.id, setdefault=best_batch)
            for task in best_tasks:
                if task == best_task:
                    continue
                self._state.set_status(task, BATCH_RUNNING, self._config)
                task.worker_running = worker_id
                task.time_running = time.time()
                self._update_task_history(task, BATCH_RUNNING, host=host)
            batch_tasks = [task.id for task in best_tasks if task != best_task]
            self._state.set_status(best_task, RUNNING, self._config, batch=batch_tasks)
            best_task.worker_running = worker_id
            best_task.time_running = time.time()
            self._update_task_history(best_task, RUNNING, host=host)

            reply['task_id'] = best_task.id
            reply['task_family'] = best_task.family
            reply['task_module'] = getattr(best_task, 'module', None)
            reply['task_params'] = best_task.params

        return reply

    def ping(self, **kwargs):
        worker_id = kwargs['worker']
        self.update(worker_id)

    def _upstream_status(self, task_id, upstream_status_table):
        if task_id in upstream_status_table:
            return upstream_status_table[task_id]
        elif self._state.has_task(task_id):
            task_stack = [task_id]

            while task_stack:
                dep_id = task_stack.pop()
                if self._state.has_task(dep_id):
                    dep = self._state.get_task(dep_id)
                    if dep.status == DONE:
                        continue
                    if dep_id not in upstream_status_table:
                        if dep.status == PENDING and dep.deps:
                            task_stack = task_stack + [dep_id] + list(dep.deps)
                            upstream_status_table[dep_id] = ''  # will be updated postorder
                        else:
                            dep_status = STATUS_TO_UPSTREAM_MAP.get(dep.status, '')
                            upstream_status_table[dep_id] = dep_status
                    elif upstream_status_table[dep_id] == '' and dep.deps:
                        # This is the postorder update step when we set the
                        # status based on the previously calculated child elements
                        upstream_status = [upstream_status_table.get(a_task_id, '') for a_task_id in dep.deps]
                        upstream_status.append('')  # to handle empty list
                        status = max(upstream_status, key=UPSTREAM_SEVERITY_KEY)
                        upstream_status_table[dep_id] = status
            return upstream_status_table[dep_id]

    def _serialize_task(self, task_id, include_deps=True, deps=None):
        task = self._state.get_task(task_id)
        ret = {
            'display_name': task.pretty_id,
            'status': task.status,
            'workers': list(task.workers),
            'worker_running': task.worker_running,
            'time_running': getattr(task, "time_running", None),
            'start_time': task.time,
            'last_updated': getattr(task, "updated", task.time),
            'params': task.params,
            'name': task.family,
            'priority': task.priority,
            'resources': task.resources,
            'tracking_url': getattr(task, "tracking_url", None),
        }
        if task.status == DISABLED:
            ret['re_enable_able'] = task.scheduler_disable_time is not None
        if include_deps:
            ret['deps'] = list(task.deps if deps is None else deps)
        return ret

    def graph(self, **kwargs):
        self.prune()
        serialized = {}
        seen = set()
        for task in self._state.get_active_tasks():
            serialized.update(self._traverse_graph(task.id, seen))
        return serialized

    def _traverse_graph(self, root_task_id, seen=None, dep_func=None):
        """ Returns the dependency graph rooted at task_id

        This does a breadth-first traversal to find the nodes closest to the
        root before hitting the scheduler.max_graph_nodes limit.

        :param root_task_id: the id of the graph's root
        :return: A map of task id to serialized node
        """

        if seen is None:
            seen = set()
        elif root_task_id in seen:
            return {}

        if dep_func is None:
            def dep_func(t):
                return t.deps

        seen.add(root_task_id)
        serialized = {}
        queue = collections.deque([root_task_id])
        while queue:
            task_id = queue.popleft()

            task = self._state.get_task(task_id)
            if task is None or not task.family:
                logger.warn('Missing task for id [%s]', task_id)

                # NOTE : If a dependency is missing from self._state there is no way to deduce the
                #        task family and parameters.
                family_match = TASK_FAMILY_RE.match(task_id)
                family = family_match.group(1) if family_match else UNKNOWN
                params = {'task_id': task_id}
                serialized[task_id] = {
                    'deps': [],
                    'status': UNKNOWN,
                    'workers': [],
                    'start_time': UNKNOWN,
                    'params': params,
                    'name': family,
                    'display_name': task_id,
                    'priority': 0,
                }
            else:
                deps = dep_func(task)
                serialized[task_id] = self._serialize_task(task_id, deps=deps)
                for dep in sorted(deps):
                    if dep not in seen:
                        seen.add(dep)
                        queue.append(dep)

            if task_id != root_task_id:
                del serialized[task_id]['display_name']
            if len(serialized) >= self._config.max_graph_nodes:
                break

        return serialized

    def dep_graph(self, task_id, **kwargs):
        self.prune()
        if not self._state.has_task(task_id):
            return {}
        return self._traverse_graph(task_id)

    def inverse_dep_graph(self, task_id, **kwargs):
        self.prune()
        if not self._state.has_task(task_id):
            return {}
        inverse_graph = collections.defaultdict(set)
        for task in self._state.get_active_tasks():
            for dep in task.deps:
                inverse_graph[dep].add(task.id)
        return self._traverse_graph(task_id, dep_func=lambda t: inverse_graph[t.id])

    def task_list(self, status, upstream_status, limit=True, search=None, **kwargs):
        """
        Query for a subset of tasks by status.
        """
        self.prune()
        result = {}
        upstream_status_table = {}  # used to memoize upstream status
        if search is None:
            def filter_func(_):
                return True
        else:
            terms = search.split()

            def filter_func(t):
                return all(term in t.pretty_id for term in terms)
        for task in filter(filter_func, self._state.get_active_tasks(status)):
            if (task.status != PENDING or not upstream_status or
                    upstream_status == self._upstream_status(task.id, upstream_status_table)):
                serialized = self._serialize_task(task.id, False)
                result[task.id] = serialized
        if limit and len(result) > self._config.max_shown_tasks:
            return {'num_tasks': len(result)}
        return result

    def _first_task_display_name(self, worker):
        task_id = worker.info.get('first_task', '')
        if self._state.has_task(task_id):
            return self._state.get_task(task_id).pretty_id
        else:
            return task_id

    def worker_list(self, include_running=True, **kwargs):
        self.prune()
        workers = [
            dict(
                name=worker.id,
                last_active=worker.last_active,
                started=getattr(worker, 'started', None),
                first_task_display_name=self._first_task_display_name(worker),
                **worker.info
            ) for worker in self._state.get_active_workers()]
        workers.sort(key=lambda worker: worker['started'], reverse=True)
        if include_running:
            running = collections.defaultdict(dict)
            num_pending = collections.defaultdict(int)
            num_uniques = collections.defaultdict(int)
            for task in self._state.get_pending_tasks():
                if task.status == RUNNING and task.worker_running:
                    running[task.worker_running][task.id] = self._serialize_task(task.id, False)
                elif task.status == PENDING:
                    for worker in task.workers:
                        num_pending[worker] += 1
                    if len(task.workers) == 1:
                        num_uniques[list(task.workers)[0]] += 1
            for worker in workers:
                tasks = running[worker['name']]
                worker['num_running'] = len(tasks)
                worker['num_pending'] = num_pending[worker['name']]
                worker['num_uniques'] = num_uniques[worker['name']]
                worker['running'] = tasks
        return workers

    def resource_list(self):
        """
        Resources usage info and their consumers (tasks).
        """
        self.prune()
        resources = [
            dict(
                name=resource,
                num_total=r_dict['total'],
                num_used=r_dict['used']
            ) for resource, r_dict in six.iteritems(self.resources())]
        if self._resources is not None:
            consumers = collections.defaultdict(dict)
            for task in self._state.get_running_tasks():
                if task.status == RUNNING and task.resources:
                    for resource, amount in six.iteritems(task.resources):
                        consumers[resource][task.id] = self._serialize_task(task.id, False)
            for resource in resources:
                tasks = consumers[resource['name']]
                resource['num_consumer'] = len(tasks)
                resource['running'] = tasks
        return resources

    def resources(self):
        ''' get total resources and available ones '''
        used_resources = self._used_resources()
        ret = collections.defaultdict(dict)
        for resource, total in self._resources.iteritems():
            ret[resource]['total'] = total
            if resource in used_resources:
                ret[resource]['used'] = used_resources[resource]
            else:
                ret[resource]['used'] = 0
        return ret

    def task_search(self, task_str, **kwargs):
        """
        Query for a subset of tasks by task_id.

        :param task_str:
        :return:
        """
        self.prune()
        result = collections.defaultdict(dict)
        for task in self._state.get_active_tasks():
            if task.id.find(task_str) != -1:
                serialized = self._serialize_task(task.id, False)
                result[task.status][task.id] = serialized
        return result

    def re_enable_task(self, task_id):
        serialized = {}
        task = self._state.get_task(task_id)
        if task and task.status == DISABLED and task.scheduler_disable_time:
            self._state.re_enable(task, self._config)
            serialized = self._serialize_task(task_id)
        return serialized

    def fetch_error(self, task_id, **kwargs):
        if self._state.has_task(task_id):
            task = self._state.get_task(task_id)
            return {"taskId": task_id, "error": task.expl, 'displayName': task.pretty_id}
        else:
            return {"taskId": task_id, "error": ""}

    def _update_task_history(self, task, status, host=None):
        try:
            if status == DONE or status == FAILED:
                successful = (status == DONE)
                self._task_history.task_finished(task, successful)
            elif status == PENDING:
                self._task_history.task_scheduled(task)
            elif status == RUNNING:
                self._task_history.task_started(task, host)
        except BaseException:
            logger.warning("Error saving Task history", exc_info=True)

    @property
    def task_history(self):
        # Used by server.py to expose the calls
        return self._task_history
