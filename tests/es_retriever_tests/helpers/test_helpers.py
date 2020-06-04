# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import unittest

from datetime import datetime, timedelta

from es_retriever.helpers.time_period import TimePeriod


class TestTimePeriod(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2018, 1, 1, 10, 00, 00)
        self.end = datetime(2018, 1, 1, 11, 10, 00)
        self.tp = TimePeriod(self.start, self.end)

    def test_instance(self):
        self.assertEqual(self.tp.start, self.start)
        self.assertEqual(self.tp.end, self.end)

    def test_split_per_day(self):
        actual_days = self.tp.split_per_day()
        expected_days = [
            TimePeriod(
                self.start,
                self.end
            ),
        ]
        self.assertEqual(actual_days, expected_days)

        other_start = datetime(2018, 1, 1, 10, 00, 00)
        other_end = datetime(2018, 1, 2, 11, 00, 00)

        self.tp.start = other_start
        self.tp.end = other_end
        actual_days = self.tp.split_per_day()
        expected_days = [
            TimePeriod(
                other_start,
                (other_start + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
            ),
            TimePeriod(
                (other_start + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                ),
                other_end
            )
        ]

        self.assertEqual(len(actual_days), len(expected_days))

        for actual, expected in zip(actual_days, expected_days):
            self.assertEqual(actual, expected)

    def test_split_per_day_full_days(self):
        actual_days = self.tp.split_per_day(full_day=True)
        expected_days = [
            TimePeriod(
                self.start,
                self.end
            ),
        ]
        print(actual_days)
        self.assertEqual(actual_days, expected_days)

        other_start = datetime(2018, 1, 1, 10, 00, 00)
        other_end = datetime(2018, 1, 2, 11, 00, 00)

        self.tp.start = other_start
        self.tp.end = other_end
        actual_days = self.tp.split_per_day(full_day=True)
        print(actual_days)
        expected_days = [
            TimePeriod(
                other_start,
                (other_start + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                ) - timedelta(seconds=1)
            ),
            TimePeriod(
                (other_start + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                ),
                other_end
            )
        ]

        self.assertEqual(len(actual_days), len(expected_days))

        for actual, expected in zip(actual_days, expected_days):
            self.assertEqual(actual, expected)
