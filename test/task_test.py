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

import doctest
from helpers import unittest
from datetime import datetime, timedelta

import luigi
import luigi.task


class DummyTask(luigi.Task):

    param = luigi.Parameter()
    bool_param = luigi.BoolParameter()
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
        other = DummyTask.from_str_params(original.to_str_params())
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

    def test_id_to_name_and_params(self):
        task_id = "InputText(date=2014-12-29)"
        (name, params) = luigi.task.id_to_name_and_params(task_id)
        self.assertEquals(name, "InputText")
        self.assertEquals(params, dict(date="2014-12-29"))

    def test_id_to_name_and_params_multiple_args(self):
        task_id = "InputText(date=2014-12-29,foo=bar)"
        (name, params) = luigi.task.id_to_name_and_params(task_id)
        self.assertEquals(name, "InputText")
        self.assertEquals(params, dict(date="2014-12-29", foo="bar"))

    def test_id_to_name_and_params_list_args(self):
        task_id = "InputText(date=2014-12-29,foo=[bar,baz-foo])"
        (name, params) = luigi.task.id_to_name_and_params(task_id)
        self.assertEquals(name, "InputText")
        self.assertEquals(params, dict(date="2014-12-29", foo=["bar", "baz-foo"]))

if __name__ == '__main__':
    unittest.main()
