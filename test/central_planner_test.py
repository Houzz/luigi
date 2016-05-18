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

import mock
import time
from helpers import unittest

from nose.plugins.attrib import attr

import luigi.notifications
from luigi.scheduler import DISABLED, DONE, FAILED, PENDING, \
    UNKNOWN, BATCH_RUNNING, CentralPlannerScheduler
from luigi.task import task_id_str

luigi.notifications.DEBUG = True
WORKER = 'myworker'


@attr('scheduler')
class CentralPlannerTest(unittest.TestCase):

    def setUp(self):
        super(CentralPlannerTest, self).setUp()
        conf = self.get_scheduler_config()
        self.sch = CentralPlannerScheduler(**conf)
        self.time = time.time

    def get_scheduler_config(self):
        return {
            'retry_delay': 100,
            'remove_delay': 1000,
            'worker_disconnect_delay': 10,
            'disable_persist': 10,
            'disable_window': 10,
            'disable_failures': 3,
            'disable_hard_timeout': 60 * 60,
        }

    def tearDown(self):
        super(CentralPlannerTest, self).tearDown()
        if time.time != self.time:
            time.time = self.time

    def setTime(self, t):
        time.time = lambda: t

    def test_dep(self):
        self.sch.add_task(worker=WORKER, task_id='B', deps=('A',))
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')
        self.sch.add_task(worker=WORKER, task_id='A', status=DONE)
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'B')
        self.sch.add_task(worker=WORKER, task_id='B', status=DONE)
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], None)

    def test_failed_dep(self):
        self.sch.add_task(worker=WORKER, task_id='B', deps=('A',))
        self.sch.add_task(worker=WORKER, task_id='A')

        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)

        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], None)  # can still wait and retry: TODO: do we want this?
        self.sch.add_task(worker=WORKER, task_id='A', status=DONE)
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'B')
        self.sch.add_task(worker=WORKER, task_id='B', status=DONE)
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], None)

    def test_broken_dep(self):
        self.sch.add_task(worker=WORKER, task_id='B', deps=('A',))
        self.sch.add_task(worker=WORKER, task_id='A', runnable=False)

        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], None)  # can still wait and retry: TODO: do we want this?
        self.sch.add_task(worker=WORKER, task_id='A', status=DONE)
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'B')
        self.sch.add_task(worker=WORKER, task_id='B', status=DONE)
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], None)

    def test_two_workers(self):
        # Worker X wants to build A -> B
        # Worker Y wants to build A -> C
        self.sch.add_task(worker='X', task_id='A')
        self.sch.add_task(worker='Y', task_id='A')
        self.sch.add_task(task_id='B', deps=('A',), worker='X')
        self.sch.add_task(task_id='C', deps=('A',), worker='Y')

        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')
        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], None)  # Worker Y is pending on A to be done
        self.sch.add_task(worker='X', task_id='A', status=DONE)
        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], 'C')
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'B')

    def test_retry(self):
        # Try to build A but fails, will retry after 100s
        self.setTime(0)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        for t in range(100):
            self.setTime(t)
            self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], None)
            self.sch.ping(worker=WORKER)
            if t % 10 == 0:
                self.sch.prune()

        self.setTime(101)
        self.sch.prune()
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')

    def test_resend_task(self):
        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.add_task(worker=WORKER, task_id='B')
        for _ in range(10):
            self.assertEqual('A', self.sch.get_work(worker=WORKER, current_tasks=[])['task_id'])
        self.assertEqual('B', self.sch.get_work(worker=WORKER, current_tasks=['A'])['task_id'])

    def test_resend_multiple_tasks(self):
        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.add_task(worker=WORKER, task_id='B')
        self.sch.add_task(worker=WORKER, task_id='C')

        # get A and B running
        self.assertEqual('A', self.sch.get_work(worker=WORKER)['task_id'])
        self.assertEqual('B', self.sch.get_work(worker=WORKER)['task_id'])

        for _ in range(10):
            self.assertEqual('A', self.sch.get_work(worker=WORKER, current_tasks=[])['task_id'])
            self.assertEqual('A', self.sch.get_work(worker=WORKER, current_tasks=['B'])['task_id'])
            self.assertEqual('B', self.sch.get_work(worker=WORKER, current_tasks=['A'])['task_id'])
            self.assertEqual('C', self.sch.get_work(worker=WORKER, current_tasks=['A', 'B'])['task_id'])

    def test_disconnect_running(self):
        # X and Y wants to run A.
        # X starts but does not report back. Y does.
        # After some timeout, Y will build it instead
        self.setTime(0)
        self.sch.add_task(task_id='A', worker='X')
        self.sch.add_task(task_id='A', worker='Y')
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')
        for t in range(200):
            self.setTime(t)
            self.sch.ping(worker='Y')
            if t % 10 == 0:
                self.sch.prune()

        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], 'A')

    def test_do_not_overwrite_tracking_url_while_running(self):
        self.sch.add_task(task_id='A', worker='X', status='RUNNING', tracking_url='trackme')
        self.assertEqual('trackme', self.sch.task_list('RUNNING', '')['A']['tracking_url'])

        # not wiped out by another working scheduling as pending
        self.sch.add_task(task_id='A', worker='Y', status='PENDING')
        self.assertEqual('trackme', self.sch.task_list('RUNNING', '')['A']['tracking_url'])

    def test_do_update_tracking_url_while_running(self):
        self.sch.add_task(task_id='A', worker='X', status='RUNNING', tracking_url='trackme')
        self.assertEqual('trackme', self.sch.task_list('RUNNING', '')['A']['tracking_url'])

        self.sch.add_task(task_id='A', worker='X', status='RUNNING', tracking_url='stage_2')
        self.assertEqual('stage_2', self.sch.task_list('RUNNING', '')['A']['tracking_url'])

    def test_keep_tracking_url_on_done_and_fail(self):
        for status in ('DONE', 'FAILED'):
            self.sch.add_task(task_id='A', worker='X', status='RUNNING', tracking_url='trackme')
            self.assertEqual('trackme', self.sch.task_list('RUNNING', '')['A']['tracking_url'])

            self.sch.add_task(task_id='A', worker='X', status=status)
            self.assertEqual('trackme', self.sch.task_list(status, '')['A']['tracking_url'])

    def test_drop_tracking_url_when_rescheduled_while_not_running(self):
        for status in ('DONE', 'FAILED', 'PENDING'):
            self.sch.add_task(task_id='A', worker='X', status=status, tracking_url='trackme')
            self.assertEqual('trackme', self.sch.task_list(status, '')['A']['tracking_url'])

            self.sch.add_task(task_id='A', worker='Y', status='PENDING')
            self.assertIsNone(self.sch.task_list('PENDING', '')['A']['tracking_url'])

    def test_reset_tracking_url_on_new_run(self):
        self.sch.add_task(task_id='A', worker='X', status='PENDING', tracking_url='trackme')
        self.assertEqual('trackme', self.sch.task_list('PENDING', '')['A']['tracking_url'])

        self.sch.add_task(task_id='A', worker='Y', status='RUNNING')
        self.assertIsNone(self.sch.task_list('RUNNING', '')['A']['tracking_url'])

    def test_remove_dep(self):
        # X schedules A -> B, A is broken
        # Y schedules C -> B: this should remove A as a dep of B
        self.sch.add_task(task_id='A', worker='X', runnable=False)
        self.sch.add_task(task_id='B', deps=('A',), worker='X')

        # X can't build anything
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], None)

        self.sch.add_task(task_id='B', deps=('C',), worker='Y')  # should reset dependencies for A
        self.sch.add_task(task_id='C', worker='Y', status=DONE)

        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], 'B')

    def test_start_time(self):
        self.setTime(100)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.setTime(200)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.add_task(worker=WORKER, task_id='A', status=DONE)
        self.assertEqual(100, self.sch.task_list(DONE, '')['A']['start_time'])

    def test_last_updated_does_not_change_with_same_status_update(self):
        for t, status in ((100, PENDING), (300, DONE), (500, DISABLED)):
            self.setTime(t)
            self.sch.add_task(worker=WORKER, task_id='A', status=status)
            self.assertEqual(t, self.sch.task_list(status, '')['A']['last_updated'])

            self.setTime(t + 100)
            self.sch.add_task(worker=WORKER, task_id='A', status=status)
            self.assertEqual(t, self.sch.task_list(status, '')['A']['last_updated'])

    def test_last_updated_shows_running_start(self):
        self.setTime(100)
        self.sch.add_task(worker=WORKER, task_id='A', status=PENDING)
        self.assertEqual(100, self.sch.task_list(PENDING, '')['A']['last_updated'])

        self.setTime(200)
        self.assertEqual('A', self.sch.get_work(worker=WORKER)['task_id'])
        self.assertEqual(200, self.sch.task_list('RUNNING', '')['A']['last_updated'])

        self.setTime(300)
        self.sch.add_task(worker=WORKER, task_id='A', status=PENDING)
        self.assertEqual(200, self.sch.task_list('RUNNING', '')['A']['last_updated'])

    def test_last_updated_with_failure_and_recovery(self):
        self.setTime(100)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertEqual('A', self.sch.get_work(worker=WORKER)['task_id'])

        self.setTime(200)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.assertEqual(200, self.sch.task_list(FAILED, '')['A']['last_updated'])

        self.setTime(1000)
        self.sch.prune()
        self.assertEqual(1000, self.sch.task_list(PENDING, '')['A']['last_updated'])

    def test_timeout(self):
        # A bug that was earlier present when restarting the same flow
        self.setTime(0)
        self.sch.add_task(task_id='A', worker='X')
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')
        self.setTime(10000)
        self.sch.add_task(task_id='A', worker='Y')  # Will timeout X but not schedule A for removal
        for i in range(2000):
            self.setTime(10000 + i)
            self.sch.ping(worker='Y')
        self.sch.add_task(task_id='A', status=DONE, worker='Y')  # This used to raise an exception since A was removed

    def test_disallowed_state_changes(self):
        # Test that we can not schedule an already running task
        t = 'A'
        self.sch.add_task(task_id=t, worker='X')
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], t)
        self.sch.add_task(task_id=t, worker='Y')
        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], None)

    def test_two_worker_info(self):
        # Make sure the scheduler returns info that some other worker is running task A
        self.sch.add_task(worker='X', task_id='A')
        self.sch.add_task(worker='Y', task_id='A')

        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')
        r = self.sch.get_work(worker='Y')
        self.assertEqual(r['task_id'], None)  # Worker Y is pending on A to be done
        s = r['running_tasks'][0]
        self.assertEqual(s['task_id'], 'A')
        self.assertEqual(s['worker'], 'X')

    def test_assistant_get_work(self):
        self.sch.add_task(worker='X', task_id='A')
        self.sch.add_worker('Y', [])

        self.assertEqual(self.sch.get_work(worker='Y', assistant=True)['task_id'], 'A')

        # check that the scheduler recognizes tasks as running
        running_tasks = self.sch.task_list('RUNNING', '')
        self.assertEqual(len(running_tasks), 1)
        self.assertEqual(list(running_tasks.keys()), ['A'])
        self.assertEqual(running_tasks['A']['worker_running'], 'Y')

    def test_assistant_get_work_external_task(self):
        self.sch.add_task(worker='X', task_id='A', runnable=False)
        self.assertTrue(self.sch.get_work(worker='Y', assistant=True)['task_id'] is None)

    def test_task_fails_when_assistant_dies(self):
        self.setTime(0)
        self.sch.add_task(worker='X', task_id='A')
        self.sch.add_worker('Y', [])

        self.assertEqual(self.sch.get_work(worker='Y', assistant=True)['task_id'], 'A')
        self.assertEqual(list(self.sch.task_list('RUNNING', '').keys()), ['A'])

        # Y dies for 50 seconds, X stays alive
        self.setTime(50)
        self.sch.ping(worker='X')
        self.assertEqual(list(self.sch.task_list('FAILED', '').keys()), ['A'])

    def test_prune_with_live_assistant(self):
        self.setTime(0)
        self.sch.add_task(worker='X', task_id='A')
        self.sch.get_work(worker='Y', assistant=True)
        self.sch.add_task(worker='Y', task_id='A', status=DONE, assistant=True)

        # worker X stops communicating, A should be marked for removal
        self.setTime(600)
        self.sch.ping(worker='Y')
        self.sch.prune()

        # A will now be pruned
        self.setTime(2000)
        self.sch.prune()
        self.assertFalse(list(self.sch.task_list('', '')))

    def test_re_enable_failed_task_assistant(self):
        self.setTime(0)
        self.sch.add_worker('X', [('assistant', True)])
        self.sch.add_task(worker='X', task_id='A', status=FAILED, assistant=True)

        # should be failed now
        self.assertEqual(FAILED, self.sch.task_list('', '')['A']['status'])

        # resets to PENDING after 100 seconds
        self.setTime(101)
        self.sch.ping(worker='X')  # worker still alive
        self.assertEqual('PENDING', self.sch.task_list('', '')['A']['status'])

    def test_fail_job_from_dead_worker_with_live_assistant(self):
        self.setTime(0)
        self.sch.add_task(worker='X', task_id='A')
        self.assertEqual('A', self.sch.get_work(worker='X')['task_id'])
        self.sch.add_worker('Y', [('assistant', True)])

        self.setTime(600)
        self.sch.ping(worker='Y')
        self.sch.prune()

        self.assertEqual(['A'], list(self.sch.task_list('FAILED', '').keys()))

    def test_assistant_request_runnable_task(self):
        self.setTime(0)
        self.sch.add_task(worker='X', task_id='A', runnable=True)
        self.setTime(600)
        self.sch.prune()

        self.assertEqual('A', self.sch.get_work(worker='Y', assistant=True)['task_id'])

    def test_assistant_request_external_task(self):
        self.sch.add_task(worker='X', task_id='A', runnable=False)
        self.assertIsNone(self.sch.get_work(worker='Y', assistant=True)['task_id'])

    def test_prune_done_tasks(self, expected=None):
        self.setTime(0)
        self.sch.add_task(worker=WORKER, task_id='A', status=DONE)
        self.sch.add_task(worker=WORKER, task_id='B', deps=['A'], status=DONE)
        self.sch.add_task(worker=WORKER, task_id='C', deps=['B'])

        self.setTime(600)
        self.sch.ping(worker='ASSISTANT')
        self.sch.prune()
        self.setTime(2000)
        self.sch.ping(worker='ASSISTANT')
        self.sch.prune()

        self.assertEqual(set(expected or ()), set(self.sch.task_list('', '').keys()))

    def test_keep_tasks_for_assistant(self):
        self.sch.get_work(worker='ASSISTANT', assistant=True)  # tell the scheduler this is an assistant
        self.test_prune_done_tasks(['B', 'C'])

    def test_keep_scheduler_disabled_tasks_for_assistant(self):
        self.sch.get_work(worker='ASSISTANT', assistant=True)  # tell the scheduler this is an assistant

        # create a scheduler disabled task and a worker disabled task
        for i in range(10):
            self.sch.add_task(worker=WORKER, task_id='D', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='E', status=DISABLED)

        # scheduler prunes the worker disabled task
        self.assertEqual(set(['D', 'E']), set(self.sch.task_list(DISABLED, '')))
        self.test_prune_done_tasks(['B', 'C', 'D'])

    def test_keep_failed_tasks_for_assistant(self):
        self.sch.get_work(worker='ASSISTANT', assistant=True)  # tell the scheduler this is an assistant
        self.sch.add_task(worker=WORKER, task_id='D', status=FAILED, deps='A')
        self.test_prune_done_tasks(['A', 'B', 'C', 'D'])

    def test_scheduler_resources_none_allow_one(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R1': 1})
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')

    def test_scheduler_resources_none_disallow_two(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R1': 2})
        self.assertFalse(self.sch.get_work(worker='X')['task_id'], 'A')

    def test_scheduler_with_insufficient_resources(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R1': 3})
        self.sch.update_resources({'R1': 2})
        self.assertFalse(self.sch.get_work(worker='X')['task_id'])

    def test_scheduler_with_sufficient_resources(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R1': 3})
        self.sch.update_resources({'R1': 3})
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')

    def test_scheduler_with_resources_used(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R1': 1})
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')

        self.sch.add_task(worker='Y', task_id='B', resources={'R1': 1})
        self.sch.update_resources({'R1': 1})
        self.assertFalse(self.sch.get_work(worker='Y')['task_id'])

    def test_scheduler_overprovisioned_on_other_resource(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R1': 2})
        self.sch.update_resources({'R1': 2})
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')

        self.sch.add_task(worker='Y', task_id='B', resources={'R2': 2})
        self.sch.update_resources({'R1': 1, 'R2': 2})
        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], 'B')

    def test_scheduler_with_priority_and_competing_resources(self):
        self.sch.add_task(worker='X', task_id='A')
        self.assertEqual(self.sch.get_work(worker='X')['task_id'], 'A')

        self.sch.add_task(worker='X', task_id='B', resources={'R': 1}, priority=10)
        self.sch.add_task(worker='Y', task_id='C', resources={'R': 1}, priority=1)
        self.sch.update_resources({'R': 1})
        self.assertFalse(self.sch.get_work(worker='Y')['task_id'])

        self.sch.add_task(worker='Y', task_id='D', priority=0)
        self.assertEqual(self.sch.get_work(worker='Y')['task_id'], 'D')

    def test_do_not_lock_resources_when_not_ready(self):
        """ Test to make sure that resources won't go unused waiting on workers """
        self.sch.add_task(worker='X', task_id='A', priority=10)
        self.sch.add_task(worker='X', task_id='B', resources={'R': 1}, priority=5)
        self.sch.add_task(worker='Y', task_id='C', resources={'R': 1}, priority=1)

        self.sch.update_resources({'R': 1})
        self.sch.add_worker('X', [('workers', 1)])
        self.assertEqual('C', self.sch.get_work(worker='Y')['task_id'])

    def test_lock_resources_when_one_of_multiple_workers_is_ready(self):
        self.sch.get_work(worker='X')  # indicate to the scheduler that X is active
        self.sch.add_task(worker='X', task_id='A', priority=10)
        self.sch.add_task(worker='X', task_id='B', resources={'R': 1}, priority=5)
        self.sch.add_task(worker='Y', task_id='C', resources={'R': 1}, priority=1)

        self.sch.update_resources({'R': 1})
        self.sch.add_worker('X', [('workers', 2)])
        self.sch.add_worker('Y', [])
        self.assertFalse(self.sch.get_work(worker='Y')['task_id'])

    def test_do_not_lock_resources_while_running_higher_priority(self):
        """ Test to make sure that resources won't go unused waiting on workers """
        self.sch.add_task(worker='X', task_id='A', priority=10)
        self.sch.add_task(worker='X', task_id='B', resources={'R': 1}, priority=5)
        self.sch.add_task(worker='Y', task_id='C', resources={'R': 1}, priority=1)

        self.sch.update_resources({'R': 1})
        self.sch.add_worker('X', [('workers', 1)])
        self.assertEqual('A', self.sch.get_work(worker='X')['task_id'])
        self.assertEqual('C', self.sch.get_work(worker='Y')['task_id'])

    def test_lock_resources_while_running_lower_priority(self):
        """ Make sure resources will be made available while working on lower priority tasks """
        self.sch.add_task(worker='X', task_id='A', priority=4)
        self.assertEqual('A', self.sch.get_work(worker='X')['task_id'])
        self.sch.add_task(worker='X', task_id='B', resources={'R': 1}, priority=5)
        self.sch.add_task(worker='Y', task_id='C', resources={'R': 1}, priority=1)

        self.sch.update_resources({'R': 1})
        self.sch.add_worker('X', [('workers', 1)])
        self.assertFalse(self.sch.get_work(worker='Y')['task_id'])

    def test_lock_resources_for_second_worker(self):
        self.sch.get_work(worker='Y')  # indicate to the scheduler that Y is active
        self.sch.add_task(worker='X', task_id='A', resources={'R': 1})
        self.sch.add_task(worker='X', task_id='B', resources={'R': 1})
        self.sch.add_task(worker='Y', task_id='C', resources={'R': 1}, priority=10)

        self.sch.add_worker('X', {'workers': 2})
        self.sch.add_worker('Y', {'workers': 1})
        self.sch.update_resources({'R': 2})

        self.assertEqual('A', self.sch.get_work(worker='X')['task_id'])
        self.assertFalse(self.sch.get_work(worker='X')['task_id'])

    def test_can_work_on_lower_priority_while_waiting_for_resources(self):
        self.sch.add_task(worker='X', task_id='A', resources={'R': 1}, priority=0)
        self.assertEqual('A', self.sch.get_work(worker='X')['task_id'])

        self.sch.add_task(worker='Y', task_id='B', resources={'R': 1}, priority=10)
        self.sch.add_task(worker='Y', task_id='C', priority=0)
        self.sch.update_resources({'R': 1})

        self.assertEqual('C', self.sch.get_work(worker='Y')['task_id'])

    @mock.patch('luigi.scheduler.configuration')
    def test_update_resources_from_config(self, configuration):
        pre_reload = {'old': 1}
        post_reload = {'refreshed': 2}
        config = configuration.get_config()

        def check_resources(resources):
            expected = dict((k, {'used': 0, 'total': v}) for k, v in resources.items())
            self.assertEqual(expected, self.sch.resources())

        config.getintdict.return_value = pre_reload
        self.sch.update_resources()
        config.getintdict.assert_called_once_with('resources')
        check_resources(pre_reload)

        def reload_config():
            config.getintdict.return_value = post_reload
        config.reload.side_effect = reload_config
        config.getintdict.reset_mock()
        self.sch.update_resources()
        config.getintdict.assert_called_once_with('resources')
        check_resources(post_reload)

    def test_priority_update_with_pruning(self):
        self.setTime(0)
        self.sch.add_task(task_id='A', worker='X')

        self.setTime(50)  # after worker disconnects
        self.sch.prune()
        self.sch.add_task(task_id='B', deps=['A'], worker='X')

        self.setTime(2000)  # after remove for task A
        self.sch.prune()

        # Here task A that B depends on is missing
        self.sch.add_task(worker=WORKER, task_id='C', deps=['B'], priority=100)
        self.sch.add_task(worker=WORKER, task_id='B', deps=['A'])
        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.add_task(worker=WORKER, task_id='D', priority=10)

        self.check_task_order('ABCD')

    def test_update_resources(self):
        self.sch.add_task(worker=WORKER, task_id='A', deps=['B'])
        self.sch.add_task(worker=WORKER, task_id='B', resources={'r': 2})
        self.sch.update_resources({'r': 1})

        # B requires too many resources, we can't schedule
        self.check_task_order([])

        self.sch.add_task(worker=WORKER, task_id='B', resources={'r': 1})

        # now we have enough resources
        self.check_task_order(['B', 'A'])

    def test_handle_multiple_resources(self):
        self.sch.add_task(worker=WORKER, task_id='A', resources={'r1': 1, 'r2': 1})
        self.sch.add_task(worker=WORKER, task_id='B', resources={'r1': 1, 'r2': 1})
        self.sch.add_task(worker=WORKER, task_id='C', resources={'r1': 1})
        self.sch.update_resources({'r1': 2, 'r2': 1})

        self.assertEqual('A', self.sch.get_work(worker=WORKER)['task_id'])
        self.check_task_order('C')

    def test_single_resource_lock(self):
        self.sch.add_task(worker='X', task_id='A', resources={'r': 1})
        self.assertEqual('A', self.sch.get_work(worker='X')['task_id'])

        self.sch.add_task(worker=WORKER, task_id='B', resources={'r': 2}, priority=10)
        self.sch.add_task(worker=WORKER, task_id='C', resources={'r': 1})
        self.sch.update_resources({'r': 2})

        # Should wait for 2 units of r to be available for B before scheduling C
        self.check_task_order([])

    def test_no_lock_if_too_many_resources_required(self):
        self.sch.add_task(worker=WORKER, task_id='A', resources={'r': 2}, priority=10)
        self.sch.add_task(worker=WORKER, task_id='B', resources={'r': 1})
        self.sch.update_resources({'r': 1})
        self.check_task_order('B')

    def test_multiple_resources_lock(self):
        self.sch.get_work(worker='X')  # indicate to the scheduler that X is active
        self.sch.add_task(worker='X', task_id='A', resources={'r1': 1, 'r2': 1}, priority=10)
        self.sch.add_task(worker=WORKER, task_id='B', resources={'r2': 1})
        self.sch.add_task(worker=WORKER, task_id='C', resources={'r1': 1})
        self.sch.update_resources({'r1': 1, 'r2': 1})

        # should preserve both resources for worker 'X'
        self.check_task_order([])

    def test_multiple_resources_no_lock(self):
        self.sch.add_task(worker=WORKER, task_id='A', resources={'r1': 1}, priority=10)
        self.sch.add_task(worker=WORKER, task_id='B', resources={'r1': 1, 'r2': 1}, priority=10)
        self.sch.add_task(worker=WORKER, task_id='C', resources={'r2': 1})
        self.sch.update_resources({'r1': 1, 'r2': 2})

        self.assertEqual('A', self.sch.get_work(worker=WORKER)['task_id'])
        # C doesn't block B, so it can go first
        self.check_task_order('C')

    def test_run_resources_while_waiting(self):
        self.sch.add_task(worker=WORKER, task_id='A', resources={'r1': 1, 'r2': 1}, priority=10)
        self.assertEqual('A', self.sch.get_work(worker=WORKER)['task_id'])

        self.sch.add_task(worker=WORKER, task_id='B', resources={'r1': 1, 'r2': 1}, priority=20)
        self.sch.add_task(worker=WORKER, task_id='C', resources={'r1': 1}, priority=0)
        self.sch.update_resources({'r1': 2, 'r2': 1})
        self.assertEqual('C', self.sch.get_work(worker=WORKER)['task_id'])

    def test_allow_resource_use_while_scheduling(self):
        self.sch.update_resources({'r1': 1})
        self.sch.add_task(worker='SCHEDULING', task_id='A', resources={'r1': 1}, priority=10)
        self.sch.add_task(worker=WORKER, task_id='B', resources={'r1': 1}, priority=1)
        self.assertEqual('B', self.sch.get_work(worker=WORKER)['task_id'])

    def test_stop_locking_resource_for_uninterested_worker(self):
        self.setTime(0)
        self.sch.update_resources({'r1': 1})
        self.assertIsNone(self.sch.get_work(worker=WORKER)['task_id'])
        self.sch.add_task(worker=WORKER, task_id='A', resources={'r1': 1}, priority=10)
        self.sch.add_task(worker='LOW_PRIO', task_id='B', resources={'r1': 1}, priority=1)
        self.assertIsNone(self.sch.get_work(worker='LOW_PRIO')['task_id'])

        self.setTime(120)
        self.assertEqual('B', self.sch.get_work(worker='LOW_PRIO')['task_id'])

    def check_task_order(self, order, sch=None):
        sch = sch or self.sch
        for expected_id in order:
            self.assertEqual(sch.get_work(worker=WORKER)['task_id'], expected_id)
            sch.add_task(worker=WORKER, task_id=expected_id, status=DONE)
        self.assertEqual(sch.get_work(worker=WORKER)['task_id'], None)

    def test_priorities(self):
        self.sch.add_task(worker=WORKER, task_id='A', priority=10)
        self.sch.add_task(worker=WORKER, task_id='B', priority=5)
        self.sch.add_task(worker=WORKER, task_id='C', priority=15)
        self.sch.add_task(worker=WORKER, task_id='D', priority=9)
        self.check_task_order(['C', 'A', 'D', 'B'])

    def test_priorities_default_and_negative(self):
        self.sch.add_task(worker=WORKER, task_id='A', priority=10)
        self.sch.add_task(worker=WORKER, task_id='B')
        self.sch.add_task(worker=WORKER, task_id='C', priority=15)
        self.sch.add_task(worker=WORKER, task_id='D', priority=-20)
        self.sch.add_task(worker=WORKER, task_id='E', priority=1)
        self.check_task_order(['C', 'A', 'E', 'B', 'D'])

    def test_priorities_and_dependencies(self):
        self.sch.add_task(worker=WORKER, task_id='A', deps=['Z'], priority=10)
        self.sch.add_task(worker=WORKER, task_id='B', priority=5)
        self.sch.add_task(worker=WORKER, task_id='C', deps=['Z'], priority=3)
        self.sch.add_task(worker=WORKER, task_id='D', priority=2)
        self.sch.add_task(worker=WORKER, task_id='Z', priority=1)
        self.check_task_order(['Z', 'A', 'B', 'C', 'D'])

    def test_priority_update_dependency_after_scheduling(self):
        self.sch.add_task(worker=WORKER, task_id='A', priority=1)
        self.sch.add_task(worker=WORKER, task_id='B', priority=5, deps=['A'])
        self.sch.add_task(worker=WORKER, task_id='C', priority=10, deps=['B'])
        self.sch.add_task(worker=WORKER, task_id='D', priority=6)
        self.check_task_order(['A', 'B', 'C', 'D'])

    def test_ranking_prefer_newer(self):
        sch = CentralPlannerScheduler(prefer_newer_tasks=True)
        sch.add_task(task_id='A', worker=WORKER)
        sch.add_task(task_id='B', worker=WORKER)
        self.check_task_order(['B', 'A'], sch)

    def test_ranking_prefer_older(self):
        sch = CentralPlannerScheduler(prefer_newer_tasks=False)
        sch.add_task(task_id='A', worker=WORKER)
        sch.add_task(task_id='B', worker=WORKER)
        self.check_task_order(['A', 'B'], sch)

    def test_disable(self):
        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)

        # should be disabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 1)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 0)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], None)

    def test_disable_and_reenable(self):
        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)

        # should be disabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 1)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 0)

        self.sch.re_enable_task('A')

        # should be enabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 0)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 1)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')

    def test_disable_and_reenable_and_disable_again(self):
        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)

        # should be disabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 1)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 0)

        self.sch.re_enable_task('A')

        # should be enabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 0)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 1)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')

        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)

        # should be still enabled
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 0)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 1)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')

        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)

        # should be disabled now
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 1)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 0)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], None)

    def test_disable_and_done(self):
        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)

        # should be disabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 1)
        self.assertEqual(len(self.sch.task_list('FAILED', '')), 0)

        self.sch.add_task(worker=WORKER, task_id='A', status=DONE)

        # should be enabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 0)
        self.assertEqual(len(self.sch.task_list('DONE', '')), 1)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')

    def test_automatic_re_enable(self):
        self.sch = CentralPlannerScheduler(disable_failures=2, disable_persist=100)
        self.setTime(0)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)

        # should be disabled now
        self.assertEqual(DISABLED, self.sch.task_list('', '')['A']['status'])

        # re-enables after 100 seconds
        self.setTime(101)
        self.assertEqual(FAILED, self.sch.task_list('', '')['A']['status'])

    def test_automatic_re_enable_with_one_failure_allowed(self):
        self.sch = CentralPlannerScheduler(disable_failures=1, disable_persist=100)
        self.setTime(0)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)

        # should be disabled now
        self.assertEqual(DISABLED, self.sch.task_list('', '')['A']['status'])

        # re-enables after 100 seconds
        self.setTime(101)
        self.assertEqual(FAILED, self.sch.task_list('', '')['A']['status'])

    def test_no_automatic_re_enable_after_manual_disable(self):
        self.sch = CentralPlannerScheduler(disable_persist=100)
        self.setTime(0)
        self.sch.add_task(worker=WORKER, task_id='A', status=DISABLED)

        # should be disabled now
        self.assertEqual(DISABLED, self.sch.task_list('', '')['A']['status'])

        # should not re-enable after 100 seconds
        self.setTime(101)
        self.assertEqual(DISABLED, self.sch.task_list('', '')['A']['status'])

    def test_no_automatic_re_enable_after_auto_then_manual_disable(self):
        self.sch = CentralPlannerScheduler(disable_failures=2, disable_persist=100)
        self.setTime(0)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)

        # should be disabled now
        self.assertEqual(DISABLED, self.sch.task_list('', '')['A']['status'])

        # should remain disabled once set
        self.sch.add_task(worker=WORKER, task_id='A', status=DISABLED)
        self.assertEqual(DISABLED, self.sch.task_list('', '')['A']['status'])

        # should not re-enable after 100 seconds
        self.setTime(101)
        self.assertEqual(DISABLED, self.sch.task_list('', '')['A']['status'])

    def test_disable_by_worker(self):
        self.sch.add_task(worker=WORKER, task_id='A', status=DISABLED)
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 1)

        self.sch.add_task(worker=WORKER, task_id='A')

        # should be enabled at this point
        self.assertEqual(len(self.sch.task_list('DISABLED', '')), 0)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')

    def test_disable_worker(self):
        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.disable_worker(worker=WORKER)
        work = self.sch.get_work(worker=WORKER)
        self.assertEqual(0, work['n_unique_pending'])
        self.assertEqual(0, work['n_pending_tasks'])
        self.assertIsNone(work['task_id'])

    def test_disable_worker_leaves_jobs_running(self):
        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.get_work(worker=WORKER)

        self.sch.disable_worker(worker=WORKER)
        self.assertEqual(['A'], list(self.sch.task_list('RUNNING', '').keys()))
        self.assertEqual(['A'], list(self.sch.worker_list()[0]['running'].keys()))

    def test_disable_worker_cannot_pick_up_failed_jobs(self):
        self.setTime(0)

        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.get_work(worker=WORKER)
        self.sch.disable_worker(worker=WORKER)
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)

        # increase time and prune to make the job pending again
        self.setTime(1000)
        self.sch.ping(worker=WORKER)
        self.sch.prune()

        # we won't try the job again
        self.assertIsNone(self.sch.get_work(worker=WORKER)['task_id'])

        # not even if other stuff is pending, changing the pending tasks code path
        self.sch.add_task(worker='other_worker', task_id='B')
        self.assertIsNone(self.sch.get_work(worker=WORKER)['task_id'])

    def test_disable_worker_cannot_continue_scheduling(self):
        self.sch.disable_worker(worker=WORKER)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertIsNone(self.sch.get_work(worker=WORKER)['task_id'])

    def test_disable_worker_can_finish_task(self, new_status=DONE, new_deps=[]):
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertEqual('A', self.sch.get_work(worker=WORKER)['task_id'])

        self.sch.disable_worker(worker=WORKER)
        self.assertEqual(['A'], list(self.sch.task_list('RUNNING', '').keys()))

        for dep in new_deps:
            self.sch.add_task(worker=WORKER, task_id=dep, status='PENDING')
        self.sch.add_task(worker=WORKER, task_id='A', status=new_status, new_deps=new_deps)
        self.assertFalse(self.sch.task_list('RUNNING', '').keys())
        self.assertEqual(['A'], list(self.sch.task_list(new_status, '').keys()))

        self.assertIsNone(self.sch.get_work(worker=WORKER)['task_id'])
        for task in self.sch.task_list('', '').values():
            self.assertFalse(task['workers'])

    def test_disable_worker_can_fail_task(self):
        self.test_disable_worker_can_finish_task(new_status=FAILED)

    def test_disable_worker_stays_disabled_on_new_deps(self):
        self.test_disable_worker_can_finish_task(new_status='PENDING', new_deps=['B', 'C'])

    def test_prune_worker(self):
        self.setTime(1)
        self.sch.add_worker(worker=WORKER, info={})
        self.setTime(10000)
        self.sch.prune()
        self.setTime(20000)
        self.sch.prune()
        self.assertFalse(self.sch.worker_list())

    def test_task_list_beyond_limit(self):
        sch = CentralPlannerScheduler(max_shown_tasks=3)
        for c in 'ABCD':
            sch.add_task(worker=WORKER, task_id=c)
        self.assertEqual(set('ABCD'), set(sch.task_list('PENDING', '', False).keys()))
        self.assertEqual({'num_tasks': 4}, sch.task_list('PENDING', ''))

    def test_task_list_within_limit(self):
        sch = CentralPlannerScheduler(max_shown_tasks=4)
        for c in 'ABCD':
            sch.add_task(worker=WORKER, task_id=c)
        self.assertEqual(set('ABCD'), set(sch.task_list('PENDING', '').keys()))

    def test_task_lists_some_beyond_limit(self):
        sch = CentralPlannerScheduler(max_shown_tasks=3)
        for c in 'ABCD':
            sch.add_task(worker=WORKER, task_id=c, status=DONE)
        for c in 'EFG':
            sch.add_task(worker=WORKER, task_id=c)
        self.assertEqual(set('EFG'), set(sch.task_list('PENDING', '').keys()))
        self.assertEqual({'num_tasks': 4}, sch.task_list('DONE', ''))

    def add_task(self, family, **params):
        task_id = str(hash((family, str(params))))  # use an unhelpful task id
        self.sch.add_task(worker=WORKER, family=family, params=params, task_id=task_id)
        return task_id

    def search_pending(self, term, expected_keys):
        actual_keys = set(self.sch.task_list('PENDING', '', search=term).keys())
        self.assertEqual(expected_keys, actual_keys)

    def test_task_list_filter_by_search_family_name(self):
        task1 = self.add_task('MySpecialTask')
        task2 = self.add_task('OtherSpecialTask')

        self.search_pending('Special', {task1, task2})
        self.search_pending('Task', {task1, task2})
        self.search_pending('My', {task1})
        self.search_pending('Other', {task2})

    def test_task_list_filter_by_search_long_family_name(self):
        task = self.add_task('TaskClassWithAVeryLongNameAndDistinctEndingUUDDLRLRAB')
        self.search_pending('UUDDLRLRAB', {task})

    def test_task_list_filter_by_param_name(self):
        task1 = self.add_task('ClassA', day='2016-02-01')
        task2 = self.add_task('ClassB', hour='2016-02-01T12')

        self.search_pending('day', {task1})
        self.search_pending('hour', {task2})

    def test_task_list_filter_by_long_param_name(self):
        task = self.add_task('ClassA', a_very_long_param_name_ending_with_uuddlrlrab='2016-02-01')

        self.search_pending('uuddlrlrab', {task})

    def test_task_list_filter_by_param_value(self):
        task1 = self.add_task('ClassA', day='2016-02-01')
        task2 = self.add_task('ClassB', hour='2016-02-01T12')

        self.search_pending('2016-02-01', {task1, task2})
        self.search_pending('T12', {task2})

    def test_task_list_filter_by_long_param_value(self):
        task = self.add_task('ClassA', param='a_very_long_param_value_ending_with_uuddlrlrab')
        self.search_pending('uuddlrlrab', {task})

    def test_task_list_filter_by_param_name_value_pair(self):
        task = self.add_task('ClassA', param='value')
        self.search_pending('param=value', {task})

    def test_task_list_does_not_filter_by_task_id(self):
        task = self.add_task('Class')
        self.search_pending(task, set())

    def test_task_list_filter_by_multiple_search_terms(self):
        expected = self.add_task('ClassA', day='2016-02-01', num='5')
        self.add_task('ClassA', day='2016-03-01', num='5')
        self.add_task('ClassB', day='2016-02-01', num='5')
        self.add_task('ClassA', day='2016-02-01', val='5')

        self.search_pending('ClassA 2016-02-01 num', {expected})

    def test_search_results_beyond_limit(self):
        sch = CentralPlannerScheduler(max_shown_tasks=3)
        for i in range(4):
            sch.add_task(worker=WORKER, family='Test', params={'p': str(i)}, task_id='Test_%i' % i)
        self.assertEqual({'num_tasks': 4}, sch.task_list('PENDING', '', search='Test'))
        self.assertEqual(['Test_0'], list(sch.task_list('PENDING', '', search='0').keys()))

    def test_priority_update_dependency_chain(self):
        self.sch.add_task(worker=WORKER, task_id='A', priority=10, deps=['B'])
        self.sch.add_task(worker=WORKER, task_id='B', priority=5, deps=['C'])
        self.sch.add_task(worker=WORKER, task_id='C', priority=1)
        self.sch.add_task(worker=WORKER, task_id='D', priority=6)
        self.check_task_order(['C', 'B', 'A', 'D'])

    def test_priority_no_decrease_with_multiple_updates(self):
        self.sch.add_task(worker=WORKER, task_id='A', priority=1)
        self.sch.add_task(worker=WORKER, task_id='B', priority=10, deps=['A'])
        self.sch.add_task(worker=WORKER, task_id='C', priority=5, deps=['A'])
        self.sch.add_task(worker=WORKER, task_id='D', priority=6)
        self.check_task_order(['A', 'B', 'D', 'C'])

    def test_aggregation(self, aggregate_type='csv', expected_args='1,3'):
        self.sch.add_task(worker=WORKER, task_id='A(a=1)', family='A', params={'a': '1'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=3)', family='A', params={'a': '3'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a')],
            batcher_aggregate_args={'a': aggregate_type})
        self.check_task_order([task_id_str('As', {'a': expected_args})])
        self.assertItemsEqual(['A(a=1)', 'A(a=3)'], self.sch.task_list('DONE', ''))

    def test_aggregation_min(self):
        self.test_aggregation('min', '1')

    def test_aggregation_max(self):
        self.test_aggregation('max', '3')

    def test_aggregation_range(self):
        self.test_aggregation('range', '1-3')

    def test_aggregation_ignores_jobs_with_pending_deps(self):
        self.sch.add_task(worker=WORKER, task_id='A(a=1)', family='A', params={'a': '1'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=2)', family='A', params={'a': '2'}, deps=['B'], batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=3)', family='A', params={'a': '3'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='B')
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a')],
            batcher_aggregate_args={'a': 'csv'})
        self.check_task_order([
            task_id_str('As', {'a': '1,3'}),
            'B',
            'A(a=2)',
        ])

    def test_aggregation_ignores_non_pending_jobs(self):
        for status in [FAILED, DONE, DISABLED]:
            self.sch.add_task(worker=WORKER, task_id='A(a=1)', family='A', params={'a': '1'}, status=PENDING, batchable=True)
            self.sch.add_task(worker=WORKER, task_id='A(a=2)', family='A', params={'a': '2'}, status=status, batchable=True)
            self.sch.add_task(worker=WORKER, task_id='A(a=3)', family='A', params={'a': '3'}, status=PENDING, batchable=True)
            self.sch.add_task_batcher(
                worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a')],
                batcher_aggregate_args={'a': 'csv'})
            self.check_task_order([task_id_str('As', {'a': '1,3'})])

    def test_aggregation_ignores_running_jobs(self):
        self.sch.add_task(worker=WORKER, task_id='A(a=1)', family='A', params={'a': '1'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=2)', family='A', params={'a': '2'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=3)', family='A', params={'a': '3'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a')],
            batcher_aggregate_args={'a': 'csv'})

        self.sch.add_task(worker='worker2', task_id='A(a=2)', family='A', params={'a': '2'}, batchable=True)
        self.assertEqual('A(a=2)', self.sch.get_work(worker='worker2')['task_id'])
        self.check_task_order([task_id_str('As', {'a': '1,3'})])

    def test_aggregation_ignores_batch_running_jobs(self):
        self.sch.add_task(worker=WORKER, task_id='A(a=1)', family='A', params={'a': '1'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=2)', family='A', params={'a': '2'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=3)', family='A', params={'a': '3'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=4)', family='A', params={'a': '4'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a')],
            batcher_aggregate_args={'a': 'csv'})

        self.sch.add_task(worker='worker2', task_id='A(a=2)', family='A', params={'a': '2'}, batchable=True)
        self.sch.add_task(worker='worker2', task_id='A(a=4)', family='A', params={'a': '4'}, batchable=True)
        self.sch.add_task_batcher(
            worker='worker2', family='A', batcher_family='As', batcher_args=[('a', 'a')],
            batcher_aggregate_args={'a': 'csv'})

        worker2_task = task_id_str('As', {'a': '2,4'})
        self.assertEqual(worker2_task, self.sch.get_work(worker='worker2')['task_id'])
        self.check_task_order([task_id_str('As', {'a': '1,3'})])

    def test_aggregate_jobs_batch_running(self):
        self.sch.add_task(worker=WORKER, task_id='A(a=1)', family='A', params={'a': '1'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=3)', family='A', params={'a': '3'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a')],
            batcher_aggregate_args={'a': 'csv'})
        self.sch.get_work(worker=WORKER)
        self.assertItemsEqual(['A(a=1)', 'A(a=3)'], self.sch.task_list(BATCH_RUNNING, ''))

    def test_aggregation_to_different_arg_names(self):
        self.sch.add_task(worker=WORKER, task_id='A(a=1)', family='A', params={'a': '1', 'b': '2'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=3)', family='A', params={'a': '3', 'b': '4'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'x'), ('b', 'y')],
            batcher_aggregate_args={'a': 'csv', 'b': 'csv'})
        self.check_task_order([task_id_str('As', {'x': '1,3', 'y': '2,4'})])

    def test_multiple_groups_same_family(self):
        self.sch.add_task(worker=WORKER, task_id='A(a=1,b=1)', family='A', params={'a': '1', 'b': '1'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=1,b=2)', family='A', params={'a': '1', 'b': '2'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=2,b=1)', family='A', params={'a': '2', 'b': '1'}, batchable=True, priority=1)
        self.sch.add_task(worker=WORKER, task_id='A(a=2,b=2)', family='A', params={'a': '2', 'b': '2'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a'), ('b', 'b')],
            batcher_aggregate_args={'b': 'csv'})
        self.check_task_order([
            task_id_str('As', {'a': '2', 'b': '1,2'}),
            task_id_str('As', {'a': '1', 'b': '1,2'}),
        ])

    def test_multiple_args_aggregate_args(self):
        self.sch.add_task(worker=WORKER, task_id='A(a=1,b=1)', family='A', params={'a': '1', 'b': '1'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=1,b=2)', family='A', params={'a': '1', 'b': '2'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=2,b=1)', family='A', params={'a': '2', 'b': '1'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='A(a=2,b=2)', family='A', params={'a': '2', 'b': '2'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a'), ('b', 'b')],
            batcher_aggregate_args={'a': 'min', 'b': 'max'})
        self.check_task_order([task_id_str('As', {'a': '1', 'b': '2'})])

    def test_batch_tasks_result_in_complete_tasks(self):
        self.test_multiple_args_aggregate_args()
        self.assertTrue(all(task['status'] == DONE for task in self.sch.task_list('', '').values()))

    def test_batch_task_size_limit(self):
        for i in range(9):
            self.sch.add_task(worker=WORKER, task_id='A(a=%i)' % i, family='A', params={'a': str(i)}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a')],
            batcher_aggregate_args={'a': 'csv'}, max_batch_size=3,
        )
        self.check_task_order([task_id_str('As', {'a': v}) for v in ['0,1,2', '3,4,5', '6,7,8']])

    def test_batch_task_size_limit_date_range(self):
        dates = ['2016-02-06', '2016-02-04', '2016-02-03', '2016-02-05']
        for date in dates:
            self.sch.add_task(worker=WORKER, task_id='A(dt=%s)' % date, params={'dt': date}, family='A', batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('dt', 'date_range')],
            batcher_aggregate_args={'dt': 'range'}, max_batch_size=3,
        )
        self.check_task_order([
            task_id_str('As', {'date_range': '2016-02-04-2016-02-06'}),
            'A(dt=2016-02-03)',
        ])

    def test_daily_overwrite_task(self):
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-01-31)', family='DOW', params={'d': '2016-01-31'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-02-01)', family='DOW', params={'d': '2016-02-01'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='DOW', batcher_family='DOW', batcher_args=[('d', 'd')],
            batcher_aggregate_args={'d': 'max'},
        )
        self.check_task_order([task_id_str('DOW', {'d': '2016-02-01'})])
        done_tasks = ['DOW(d=2016-01-31)', 'DOW(d=2016-02-01)']
        self.assertItemsEqual(done_tasks, self.sch.task_list(DONE, ''))

    def test_daily_overwrite_task_failure(self):
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-01-31)', family='DOW', params={'d': '2016-01-31'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-02-01)', family='DOW', params={'d': '2016-02-01'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='DOW', batcher_family='DOW', batcher_args=[('d', 'd')],
            batcher_aggregate_args={'d': 'max'},
        )
        expected_id = task_id_str('DOW', {'d': '2016-02-01'})
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], expected_id)
        self.sch.add_task(worker=WORKER, task_id=expected_id, status=FAILED)
        failed_tasks = ['DOW(d=2016-01-31)', 'DOW(d=2016-02-01)']
        self.assertItemsEqual(failed_tasks, self.sch.task_list(FAILED, ''))

    def test_daily_overwrite_task_auto_disable(self):
        sch = CentralPlannerScheduler(disable_failures=2, disable_persist=10**8, disable_window=10**8)
        sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='A', batcher_args=[('i', 'i')],
            batcher_aggregate_args={'i': 'max'},
        )
        all_tasks = [task_id_str('A', {'i': str(i)}) for i in range(5)]
        batch_running = set(all_tasks[:-1])

        for _ in range(2):
            for i, task_id in enumerate(all_tasks):
                sch.add_task(worker=WORKER, task_id=task_id, family='A', params={'i': str(i)}, batchable=True, status=PENDING)
            running_task = task_id_str('A', {'i': '4'})
            self.assertEqual(running_task, sch.get_work(worker=WORKER)['task_id'])
            self.assertEqual(batch_running, set(sch.task_list(BATCH_RUNNING, '')))
            sch.add_task(worker=WORKER, task_id=running_task, status=FAILED)
            self.assertEqual(set(), set(sch.task_list(BATCH_RUNNING, '')))

        self.assertEqual(set(all_tasks), set(sch.task_list(DISABLED, '')))
        self.assertTrue(all(task['re_enable_able'] for task in sch.task_list(DISABLED, '').values()))

    def test_batch_task_does_not_batch_jobs_from_other_workers(self):
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-01-31)', family='DOW', params={'d': '2016-01-31'}, batchable=True, resources={'a': 1})
        self.sch.add_task(worker='OTHER_WORKER', task_id='DOW(d=2016-02-01)', family='DOW', params={'d': '2016-02-01'}, batchable=True, resources={'a': 1})
        self.sch.add_task_batcher(
            worker=WORKER, family='DOW', batcher_family='DOW', batcher_args=[('d', 'd')],
            batcher_aggregate_args={'d': 'max'},
        )
        done_tasks = {'DOW(d=2016-01-31)'}
        self.check_task_order(done_tasks)
        self.assertEqual(done_tasks, set(self.sch.task_list(DONE, '')))

    def test_done_batch_tasks_fall_out_of_scheduler(self):
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-01-31)', family='DOW', params={'d': '2016-01-31'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-02-01)', family='DOW', params={'d': '2016-02-01'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='DOW', batcher_family='DowBatch', batcher_args=[('d', 'd')],
            batcher_aggregate_args={'d': 'max'},
        )
        self.check_task_order([task_id_str('DowBatch', {'d': '2016-02-01'})])
        done_tasks = ['DOW(d=2016-01-31)', 'DOW(d=2016-02-01)']
        self.assertItemsEqual(done_tasks, self.sch.task_list(DONE, ''))

    def test_failed_batch_tasks_fall_out_of_scheduler(self):
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-01-31)', family='DOW', params={'d': '2016-01-31'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-02-01)', family='DOW', params={'d': '2016-02-01'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='DOW', batcher_family='DowBatch', batcher_args=[('d', 'd')],
            batcher_aggregate_args={'d': 'max'},
        )
        expected_id = task_id_str('DowBatch', {'d': '2016-02-01'})
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], expected_id)
        self.sch.add_task(worker=WORKER, task_id=expected_id, status=FAILED)
        failed_tasks = ['DOW(d=2016-01-31)', 'DOW(d=2016-02-01)']
        self.assertItemsEqual(failed_tasks, self.sch.task_list(FAILED, ''))

    def test_batch_task_update_tracking_url(self):
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-01-31)', family='DOW', params={'d': '2016-01-31'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-02-01)', family='DOW', params={'d': '2016-02-01'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='DOW', batcher_family='DowBatch', batcher_args=[('d', 'd')],
            batcher_aggregate_args={'d': 'max'},
        )
        expected_id = task_id_str('DowBatch', {'d': '2016-02-01'})
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], expected_id)
        tracking_url = 'http://sample.url/'
        self.sch.add_task(worker=WORKER, task_id=expected_id, status='RUNNING', tracking_url=tracking_url)

        expected_batch = ['DOW(d=2016-01-31)', 'DOW(d=2016-02-01)']
        batch_tasks = self.sch.task_list(BATCH_RUNNING, '')
        self.assertItemsEqual(expected_batch, batch_tasks.keys())
        self.assertTrue(all(task['tracking_url'] == tracking_url for task in batch_tasks.values()))

        running_tasks = self.sch.task_list('RUNNING', '')
        self.assertItemsEqual([expected_id], running_tasks.keys())
        self.assertTrue(all(task['tracking_url'] == tracking_url for task in running_tasks.values()))

    def test_batch_tasks_pruned_from_dead_worker(self):
        self.setTime(1)
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-01-31)', family='DOW', params={'d': '2016-01-31'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='DOW(d=2016-02-01)', family='DOW', params={'d': '2016-02-01'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='DOW', batcher_family='DowBatch', batcher_args=[('d', 'd')],
            batcher_aggregate_args={'d': 'max'},
        )
        expected_id = task_id_str('DowBatch', {'d': '2016-02-01'})
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], expected_id)
        self.setTime(1000)
        self.sch.prune()
        failed_tasks = ['DOW(d=2016-01-31)', 'DOW(d=2016-02-01)']
        self.assertItemsEqual(failed_tasks, self.sch.task_list(FAILED, ''))

    def test_batch_task_ignore_unimportant_param(self):
        self.sch.add_task(worker=WORKER, task_id='Refresh(table=A, time=1)', family='Refresh', params={'table': 'A', 'time': '1'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='Refresh(table=A, time=2)', family='Refresh', params={'table': 'A', 'time': '2'}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='Refresh',
            batcher_family='RefreshBatch', batcher_args=[('table', 'table')],
            batcher_aggregate_args={},
        )
        self.check_task_order([task_id_str('RefreshBatch', {'table': 'A'})])

    def test_batch_task_combines_resources(self):
        self.sch.add_task(worker=WORKER, task_id='A', family='A', params={'a': '1'}, resources={'a': 1}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='B', family='A', params={'a': '2'}, resources={'b': 1}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a')],
            batcher_aggregate_args={'a': 'csv'},
        )
        self.assertEqual(task_id_str('As', {'a': '1,2'}), self.sch.get_work(worker=WORKER)['task_id'])

        # add tasks using a and b to make sure they can't schedule
        self.sch.add_task(worker=WORKER, task_id='C', resources={'a': 1}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='D', resources={'b': 1}, batchable=True)
        self.assertIsNone(self.sch.get_work(worker=WORKER)['task_id'])

    def test_batch_task_takes_max_resource_values(self):
        self.sch.add_task(worker=WORKER, task_id='A', family='A', params={'a': '1'}, resources={'a': 1}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='B', family='A', params={'a': '2'}, resources={'a': 2}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a')],
            batcher_aggregate_args={'a': 'csv'},
        )
        self.sch.update_resources({'a': 2})
        self.assertEqual(task_id_str('As', {'a': '1,2'}), self.sch.get_work(worker=WORKER)['task_id'])
        self.assertEqual({'a': {'total': 2, 'used': 2}}, self.sch.resources())

    def test_batch_task_excludes_items_with_too_many_resources(self):
        self.sch.add_task(worker=WORKER, task_id='A', family='A', params={'a': '1'}, resources={'a': 1}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='B', family='A', params={'a': '2'}, resources={'a': 2}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='C', family='A', params={'a': '3'}, resources={'a': 1}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a')],
            batcher_aggregate_args={'a': 'csv'},
        )
        self.sch.update_resources({'a': 1})
        self.assertEqual(task_id_str('As', {'a': '1,3'}), self.sch.get_work(worker=WORKER)['task_id'])

    def test_batch_task_not_used_for_single_task(self):
        self.sch.add_task(worker=WORKER, task_id='A', family='A', params={'a': '1'}, resources={'a': 1}, batchable=True)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a')],
            batcher_aggregate_args={'a': 'csv'},
        )
        self.assertEqual('A', self.sch.get_work(worker=WORKER)['task_id'])

    def test_batch_task_not_all_batchable(self):
        self.sch.add_task(worker=WORKER, task_id='A', family='A', params={'a': '1'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='B', family='A', params={'a': '2'}, batchable=True)
        self.sch.add_task(worker=WORKER, task_id='C', family='A', params={'a': '3'}, batchable=False)
        self.sch.add_task_batcher(
            worker=WORKER, family='A', batcher_family='As', batcher_args=[('a', 'a')],
            batcher_aggregate_args={'a': 'csv'},
        )
        self.check_task_order([task_id_str('As', {'a': '1,2'}), 'C'])

    def test_unique_tasks(self):
        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.add_task(worker=WORKER, task_id='B')
        self.sch.add_task(worker=WORKER, task_id='C')
        self.sch.add_task(worker=WORKER + "_2", task_id='B')

        response = self.sch.get_work(worker=WORKER)
        self.assertEqual(3, response['n_pending_tasks'])
        self.assertEqual(2, response['n_unique_pending'])

    def test_pending_downstream_disable(self):
        self.sch.add_task(worker=WORKER, task_id='A', status=DISABLED)
        self.sch.add_task(worker=WORKER, task_id='B', deps=('A',))
        self.sch.add_task(worker=WORKER, task_id='C', deps=('B',))

        response = self.sch.get_work(worker=WORKER)
        self.assertTrue(response['task_id'] is None)
        self.assertEqual(0, response['n_pending_tasks'])
        self.assertEqual(0, response['n_unique_pending'])

    def test_pending_downstream_failure(self):
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='B', deps=('A',))
        self.sch.add_task(worker=WORKER, task_id='C', deps=('B',))

        response = self.sch.get_work(worker=WORKER)
        self.assertTrue(response['task_id'] is None)
        self.assertEqual(2, response['n_pending_tasks'])
        self.assertEqual(2, response['n_unique_pending'])

    def test_task_list_no_deps(self):
        self.sch.add_task(worker=WORKER, task_id='B', deps=('A',))
        self.sch.add_task(worker=WORKER, task_id='A')
        task_list = self.sch.task_list('PENDING', '')
        self.assertFalse('deps' in task_list['A'])

    def test_task_first_failure_time(self):
        self.sch.add_task(worker=WORKER, task_id='A')
        test_task = self.sch._state.get_task('A')
        self.assertIsNone(test_task.failures.first_failure_time)

        time_before_failure = time.time()
        test_task.add_failure()
        time_after_failure = time.time()

        self.assertLessEqual(time_before_failure,
                             test_task.failures.first_failure_time)
        self.assertGreaterEqual(time_after_failure,
                                test_task.failures.first_failure_time)

    def test_task_first_failure_time_remains_constant(self):
        self.sch.add_task(worker=WORKER, task_id='A')
        test_task = self.sch._state.get_task('A')
        self.assertIsNone(test_task.failures.first_failure_time)

        test_task.add_failure()
        first_failure_time = test_task.failures.first_failure_time

        test_task.add_failure()
        self.assertEqual(first_failure_time, test_task.failures.first_failure_time)

    def test_task_has_excessive_failures(self):
        self.sch.add_task(worker=WORKER, task_id='A')
        test_task = self.sch._state.get_task('A')
        self.assertIsNone(test_task.failures.first_failure_time)

        self.assertFalse(test_task.has_excessive_failures())

        test_task.add_failure()
        self.assertFalse(test_task.has_excessive_failures())

        fake_failure_time = (test_task.failures.first_failure_time -
                             2 * 60 * 60)

        test_task.failures.first_failure_time = fake_failure_time
        self.assertTrue(test_task.has_excessive_failures())

    def test_quadratic_behavior(self):
        """ Test that get_work is not taking linear amount of time.

        This is of course impossible to test, however, doing reasonable
        assumptions about hardware. This time should finish in a timely
        manner.
        """
        # For 10000 it takes almost 1 second on my laptop.  Prior to these
        # changes it was being slow already at NUM_TASKS=300
        NUM_TASKS = 10000
        for i in range(NUM_TASKS):
            self.sch.add_task(worker=str(i), task_id=str(i), resources={})

        for i in range(NUM_TASKS):
            self.assertEqual(self.sch.get_work(worker=str(i))['task_id'], str(i))
            self.sch.add_task(worker=str(i), task_id=str(i), status=DONE)

    def test_get_work_speed(self):
        """ Test that get_work is fast for few workers and many DONEs.

        In #986, @daveFNbuck reported that he got a slowdown.
        """
        # This took almost 4 minutes without optimization.
        # Now it takes 10 seconds on my machine.
        NUM_PENDING = 1000
        NUM_DONE = 200000
        assert NUM_DONE >= NUM_PENDING
        for i in range(NUM_PENDING):
            self.sch.add_task(worker=WORKER, task_id=str(i), resources={})

        for i in range(NUM_PENDING, NUM_DONE):
            self.sch.add_task(worker=WORKER, task_id=str(i), status=DONE)

        for i in range(NUM_PENDING):
            res = int(self.sch.get_work(worker=WORKER)['task_id'])
            self.assertTrue(0 <= res < NUM_PENDING)
            self.sch.add_task(worker=WORKER, task_id=str(res), status=DONE)

    def test_assistants_dont_nurture_finished_statuses(self):
        """
        Assistants should not affect longevity of DONE tasks

        Also check for statuses DISABLED and UNKNOWN.
        """
        self.sch = CentralPlannerScheduler(retry_delay=100000000000)  # Never pendify failed tasks
        self.setTime(1)
        self.sch.add_worker('assistant', [('assistant', True)])
        self.sch.ping(worker='assistant')
        self.sch.add_task(worker='uploader', task_id='running', status=PENDING)
        self.assertEqual(self.sch.get_work(worker='assistant', assistant=True)['task_id'], 'running')

        self.setTime(2)
        self.sch.add_task(worker='uploader', task_id='done', status=DONE)
        self.sch.add_task(worker='uploader', task_id='disabled', status=DISABLED)
        self.sch.add_task(worker='uploader', task_id='pending', status=PENDING)
        self.sch.add_task(worker='uploader', task_id='failed', status=FAILED)
        self.sch.add_task(worker='uploader', task_id='unknown', status=UNKNOWN)

        self.setTime(100000)
        self.sch.ping(worker='assistant')
        self.sch.prune()

        self.setTime(200000)
        self.sch.ping(worker='assistant')
        self.sch.prune()
        nurtured_statuses = ['PENDING', 'FAILED', 'RUNNING']
        not_nurtured_statuses = ['DONE', 'UNKNOWN', 'DISABLED']

        for status in nurtured_statuses:
            print(status)
            self.assertEqual(set([status.lower()]), set(self.sch.task_list(status, '')))

        for status in not_nurtured_statuses:
            print(status)
            self.assertEqual(set([]), set(self.sch.task_list(status, '')))

        self.assertEqual(3, len(self.sch.task_list(None, '')))  # None == All statuses

    def test_no_crash_on_only_disable_hard_timeout(self):
        """
        Scheduler shouldn't crash with only disable_hard_timeout

        There was some failure happening when disable_hard_timeout was set but
        disable_failures was not.
        """
        self.sch = CentralPlannerScheduler(retry_delay=5,
                                           disable_hard_timeout=100)
        self.setTime(1)
        self.sch.add_worker(WORKER, [])
        self.sch.ping(worker=WORKER)

        self.setTime(2)
        self.sch.add_task(worker=WORKER, task_id='A')
        self.sch.add_task(worker=WORKER, task_id='B', deps=['A'])
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')
        self.sch.add_task(worker=WORKER, task_id='A', status=FAILED)
        self.setTime(10)
        self.sch.prune()
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')
