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

from helpers import unittest

from helpers import with_config
from luigi import notifications
from luigi import six


class TestEmail(unittest.TestCase):

    def testEmailNoPrefix(self):
        self.assertEqual("subject", notifications._prefix('subject'))

    @with_config({"core": {"email-prefix": "[prefix]"}})
    def testEmailPrefix(self):
        self.assertEqual("[prefix] subject", notifications._prefix('subject'))

    @with_config({"core": {"error-email": "a@a.a"}})
    def testEmailRecipients(self):
        six.assertCountEqual(self, notifications._email_recipients(), ["a@a.a"])
        six.assertCountEqual(self, notifications._email_recipients("b@b.b"), ["a@a.a", "b@b.b"])
        six.assertCountEqual(self, notifications._email_recipients(["b@b.b", "c@c.c"]),
                             ["a@a.a", "b@b.b", "c@c.c"])

    @with_config({"core": {}}, replace_sections=True)
    def testEmailRecipientsNoConfig(self):
        six.assertCountEqual(self, notifications._email_recipients(), [])
        six.assertCountEqual(self, notifications._email_recipients("a@a.a"), ["a@a.a"])
        six.assertCountEqual(self, notifications._email_recipients(["a@a.a", "b@b.b"]),
                             ["a@a.a", "b@b.b"])
