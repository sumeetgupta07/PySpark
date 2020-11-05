import unittest

from jobs.recipe import *


class DurationFunctionsTest(unittest.TestCase):

    def test_get_seconds_from_duration_success(self):
        seconds = int(get_seconds_from_duration("PT1H"))
        self.assertEqual(seconds, 3600)

    def test_get_seconds_from_duration_Fail(self):
        seconds = int(get_seconds_from_duration("PT"))
        self.assertEqual(seconds, -10)

    def test_get_duration_from_seconds(self):
        duration = str(get_duration_from_seconds(1800))
        self.assertEqual(duration, "PT30M")

    def test_get_duration_from_seconds_fail(self):
        duration = str(get_duration_from_seconds(-1))
        self.assertEqual(duration, "")

    def test_get_duration_from_seconds_empty_fail(self):
        duration = str(get_duration_from_seconds(""))
        self.assertEqual(duration, "")
