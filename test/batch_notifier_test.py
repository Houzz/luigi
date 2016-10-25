import mock
import unittest

from luigi.batch_notifier import BatchNotifier


class BatchNotifierTest(unittest.TestCase):
    def setUp(self):
        self.time_mock = mock.patch('luigi.batch_notifier.time.time')
        self.time = self.time_mock.start()
        self.time.return_value = 0.0

        self.send_email_mock = mock.patch('luigi.batch_notifier.send_error_email')
        self.send_email = self.send_email_mock.start()

    def tearDown(self):
        self.time_mock.stop()
        self.send_email.stop()

    def incr_time(self, minutes):
        self.time.return_value += minutes * 60

    def test_send_single_failure(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error')
        bn.send_email()
        self.send_email.assert_called_once_with(
            '1 failure in the last 60 minutes',
            'Task(a=5) (1 failure)'
        )

    def test_send_single_disable(self):
        bn = BatchNotifier(batch_mode='all')
        for _ in range(10):
            bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error')
        bn.add_disable('Task(a=5)', 'Task', {'a': 5})
        bn.send_email()
        self.send_email.assert_called_once_with(
            '10 failures in the last 60 minutes',
            'Task(a=5) (10 failures, 1 disable)'
        )

    def test_send_multiple_disables(self):
        bn = BatchNotifier(batch_mode='family')
        for _ in range(10):
            bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error')
            bn.add_failure('Task(a=6)', 'Task', {'a': 6}, 'error')
        bn.add_disable('Task(a=5)', 'Task', {'a': 5})
        bn.add_disable('Task(a=6)', 'Task', {'a': 6})
        bn.send_email()
        self.send_email.assert_called_once_with(
            '20 failures in the last 60 minutes',
            'Task (20 failures, 2 disables)'
        )

    def test_multiple_failures_of_same_job(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error')
        bn.send_email()
        self.send_email.assert_called_once_with(
            '3 failures in the last 60 minutes',
            'Task(a=5) (3 failures)'
        )

    def test_multiple_failures_of_multiple_jobs(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error')
        bn.add_failure('Task(a=6)', 'Task', {'a': 6}, 'error')
        bn.add_failure('Task(a=6)', 'Task', {'a': 6}, 'error')
        bn.send_email()
        self.send_email.assert_called_once_with(
            '3 failures in the last 60 minutes',
            'Task(a=6) (2 failures)\n'
            'Task(a=5) (1 failure)'
        )

    def test_group_on_family(self):
        bn = BatchNotifier(batch_mode='family')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error')
        bn.add_failure('Task(a=6)', 'Task', {'a': 6}, 'error')
        bn.add_failure('Task(a=6)', 'Task', {'a': 6}, 'error')
        bn.add_failure('OtherTask(a=6)', 'OtherTask', {'a': 6}, 'error')
        bn.send_email()
        self.send_email.assert_called_once_with(
            '4 failures in the last 60 minutes',
            'Task (3 failures)\n'
            'OtherTask (1 failure)'
        )

    def test_group_on_unbatched_params(self):
        bn = BatchNotifier(batch_mode='unbatched_params')
        bn.add_failure('Task(a=5, b=1)', 'Task', {'a': 5}, 'error')
        bn.add_failure('Task(a=5, b=2)', 'Task', {'a': 5}, 'error')
        bn.add_failure('Task(a=6, b=1)', 'Task', {'a': 6}, 'error')
        bn.add_failure('Task(a=6, b=2)', 'Task', {'a': 6}, 'error')
        bn.add_failure('Task(a=6, b=3)', 'Task', {'a': 6}, 'error')
        bn.add_failure('Task(a=6, b=4)', 'Task', {'a': 6}, 'error')
        bn.add_failure('OtherTask(a=5, b=1)', 'OtherTask', {'a': 5}, 'error')
        bn.add_failure('OtherTask(a=6, b=1)', 'OtherTask', {'a': 6}, 'error')
        bn.add_failure('OtherTask(a=6, b=2)', 'OtherTask', {'a': 6}, 'error')
        bn.add_failure('OtherTask(a=6, b=3)', 'OtherTask', {'a': 6}, 'error')
        bn.send_email()
        self.send_email.assert_called_once_with(
            '10 failures in the last 60 minutes',
            'Task(a=6) (4 failures)\n'
            'OtherTask(a=6) (3 failures)\n'
            'Task(a=5) (2 failures)\n'
            'OtherTask(a=5) (1 failure)'
        )

    def test_send_clears_backlog(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error')
        bn.add_disable('Task(a=5)', 'Task', {'a': 5})
        bn.send_email()

        self.send_email.reset_mock()
        bn.send_email()
        self.send_email.assert_not_called()

    def test_auto_send_on_update_after_time_period(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error')

        for i in range(60):
            bn.update()
            self.send_email.assert_not_called()
            self.incr_time(minutes=1)

        bn.update()
        self.send_email.assert_called_once_with(
            '1 failure in the last 60 minutes',
            'Task(a=5) (1 failure)'
        )

    def test_auto_send_on_update_after_time_period_with_disable_only(self):
        bn = BatchNotifier(batch_mode='all')
        bn.add_disable('Task(a=5)', 'Task', {'a': 5})

        for i in range(60):
            bn.update()
            self.send_email.assert_not_called()
            self.incr_time(minutes=1)

        bn.update()
        self.send_email.assert_called_once_with(
            '0 failures in the last 60 minutes',
            'Task(a=5) (0 failures, 1 disable)'
        )

    def test_no_auto_send_until_end_of_interval_with_error(self):
        bn = BatchNotifier(batch_mode='all')

        for i in range(90):
            bn.update()
            self.send_email.assert_not_called()
            self.incr_time(minutes=1)

        bn.add_failure('Task(a=5)', 'Task', {'a': 5}, 'error')
        for i in range(30):
            bn.update()
            self.send_email.assert_not_called()
            self.incr_time(minutes=1)

        bn.update()
        self.send_email.assert_called_once_with(
            '1 failure in the last 60 minutes',
            'Task(a=5) (1 failure)'
        )
