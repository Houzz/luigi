import doctest
import unittest

import luigi.task
import luigi
from datetime import datetime, timedelta


class DummyTask(luigi.Task):

    param = luigi.Parameter()
    bool_param = luigi.BooleanParameter()
    int_param = luigi.IntParameter()
    float_param = luigi.FloatParameter()
    date_param = luigi.DateParameter()
    datehour_param = luigi.DateHourParameter()
    timedelta_param = luigi.TimeDeltaParameter()
    list_param = luigi.Parameter(is_list=True)


class TaskTest(unittest.TestCase):

    def test_tasks_doctest(self):
        doctest.testmod(luigi.task)

    def test_task_to_str_to_task(self):
        params = dict(
            param='test',
            bool_param=True,
            int_param=666,
            float_param=123.456,
            date_param=datetime(2014, 9, 13).date(),
            datehour_param=datetime(2014, 9, 13, 9),
            timedelta_param=timedelta(44),  # doesn't support seconds
            list_param=['in', 'flames'])

        original = DummyTask(**params)
        other = DummyTask.from_str_params(original.to_str_params(), {})
        self.assertEqual(original, other)

    def test_undo_without_remove(self):
        class TestTask(luigi.Task):
            def output(self):
                return object()

        self.assertRaises(NotImplementedError, TestTask().undo)

    def test_undo_overridden_complete_function(self):
        class TestTask(luigi.Task):
            def complete(self):
                return True

        self.assertRaises(NotImplementedError, TestTask().undo)

    def test_undo_do_not_remove_files_that_dont_exist(self):
        class RemoveErrorOutput(object):
            def exists(self):
                return False
            def remove(self):
                assert False

        class TestTask(luigi.Task):
            def output(self):
                return RemoveErrorOutput()

        TestTask().undo()

    def test_undo_remove_existing_file(self):
        class RemovableFile(object):
            _exists = True
            def exists(self):
                return self._exists
            def remove(self):
                self._exists = False

        class TestTask(luigi.Task):
            _output = RemovableFile()
            def output(self):
                return self._output

        t = TestTask()
        self.assertTrue(t.complete())
        t.undo()
        self.assertFalse(t.complete())

    def test_undo_mixed_existing_files(self):
        class RemovableFile(object):
            def __init__(self, i):
                self._exists = i % 3 > 0
            def exists(self):
                return self._exists
            def remove(self):
                self._exists = False

        class TestTask(luigi.Task):
            _output = map(RemovableFile, range(11))
            def output(self):
                return self._output

        t = TestTask()
        self.assertEqual(7, sum(f.exists() for f in t.output()))
        t.undo()
        self.assertFalse(any(f.exists() for f in t.output()))

if __name__ == '__main__':
    unittest.main()
