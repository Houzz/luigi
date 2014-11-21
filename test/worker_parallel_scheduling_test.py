import multiprocessing
import time
import unittest
import mock

import luigi

from luigi.worker import Worker


class SlowCompleteWrapper(luigi.WrapperTask):
    def requires(self):
        return [SlowCompleteTask(i) for i in range(4)]


class SlowCompleteTask(luigi.Task):
    n = luigi.IntParameter()

    def complete(self):
        time.sleep(0.1)
        return True


class OverlappingSelfDependenciesTask(luigi.Task):
    n = luigi.IntParameter()
    k = luigi.IntParameter()

    def complete(self):
        return self.n < self.k or self.k == 0

    def requires(self):
        return [OverlappingSelfDependenciesTask(self.n-1, k) for k in range(self.k+1)]


class ExceptionCompleteTask(luigi.Task):
    def complete(self):
        assert False


class ExceptionRequiresTask(luigi.Task):
    def requires(self):
        assert False


class ParallelSchedulingTest(unittest.TestCase):
    def setUp(self):
        self.sch = mock.Mock()
        self.w = Worker(scheduler=self.sch, worker_id='x')

    def add_time(self, task, skip_root, multiprocess):
        start_time = time.time()
        self.w.add(task, skip_root, multiprocess)
        end_time = time.time()
        return end_time - start_time

    def added_tasks(self, status):
        return [args[1] for args, kw in self.sch.add_task.call_args_list if kw['status'] == status]

    @unittest.skipIf(multiprocessing.cpu_count() <= 1,
                     "Cannot test multiprocess scheduling with only one processor")
    def test_speed_up_scheduling_on_slow_complete(self):
        self.assertLess(0.4, self.add_time(SlowCompleteWrapper(), True, False))
        self.assertEqual(self.sch.add_task.call_count, 4)
        expected_tasks = ['SlowCompleteTask(n=%i)' % i for i in range(4)]
        self.assertItemsEqual(expected_tasks, self.added_tasks('DONE'))

        self.sch.add_task.reset_mock()
        self.assertGreater(0.4, self.add_time(SlowCompleteWrapper(), True, True))
        self.assertEqual(self.sch.add_task.call_count, 4)
        self.assertItemsEqual(expected_tasks, self.added_tasks('DONE'))

    def test_multiprocess_scheduling_with_overlapping_dependencies(self):
        self.w.add(OverlappingSelfDependenciesTask(5, 2), False, True)
        self.assertEqual(15, self.sch.add_task.call_count)
        self.assertItemsEqual((
            'OverlappingSelfDependenciesTask(n=1, k=1)',
            'OverlappingSelfDependenciesTask(n=2, k=1)',
            'OverlappingSelfDependenciesTask(n=2, k=2)',
            'OverlappingSelfDependenciesTask(n=3, k=1)',
            'OverlappingSelfDependenciesTask(n=3, k=2)',
            'OverlappingSelfDependenciesTask(n=4, k=1)',
            'OverlappingSelfDependenciesTask(n=4, k=2)',
            'OverlappingSelfDependenciesTask(n=5, k=2)',
        ), self.added_tasks('PENDING'))
        self.assertItemsEqual((
            'OverlappingSelfDependenciesTask(n=0, k=0)',
            'OverlappingSelfDependenciesTask(n=0, k=1)',
            'OverlappingSelfDependenciesTask(n=1, k=0)',
            'OverlappingSelfDependenciesTask(n=1, k=2)',
            'OverlappingSelfDependenciesTask(n=2, k=0)',
            'OverlappingSelfDependenciesTask(n=3, k=0)',
            'OverlappingSelfDependenciesTask(n=4, k=0)',
        ), self.added_tasks('DONE'))


    @mock.patch('luigi.notifications.send_error_email')
    def test_raise_exception_in_complete(self, send):
        self.w.add(ExceptionCompleteTask(), multiprocess=True)
        send.assert_called_once()
        self.assertEqual(0, self.sch.add_task.call_count)

    @mock.patch('luigi.notifications.send_error_email')
    def test_raise_exception_in_requires(self, send):
        self.w.add(ExceptionRequiresTask(), multiprocess=True)
        send.assert_called_once()
        self.assertEqual(0, self.sch.add_task.call_count)

    @mock.patch('luigi.notifications.send_error_email')
    def test_raise_exception_in_requires_with_skip_roopt(self, send):
        self.w.add(ExceptionRequiresTask(), skip_root=True, multiprocess=True)
        send.assert_called_once()
        self.assertEqual(0, self.sch.add_task.call_count)


if __name__ == '__main__':
    unittest.main()
