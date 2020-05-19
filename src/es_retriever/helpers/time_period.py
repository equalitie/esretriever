from datetime import timedelta

ENDCOLOR = '\033[0m'


class TimePeriod(object):
    """
    Represents a time period, with helper functions
    """
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __str__(self):
        return 'TimePeriod from {} to {}'.format(self.start, self.end)

    def __repr__(self):
        return '<{}>'.format(self)

    def __eq__(self, other):
        return self.start == other.start and self.end == other.end

    def __ne__(self, other):
        return self.start != other.start or self.end != other.end

    def split_per_day(self, full_day=False):
        """
        Splits the time period in days
        :rtype: list[TimePeriod]
        :return: a list that contains time periods that when combined together
        they amount to the initial / current period of time
        """
        days = []

        start = self.start
        end = (start + timedelta(days=1)).replace(
                hour=00, minute=00, second=00, microsecond=00
            )
        if full_day:
            end = end-timedelta(seconds=1)

        while True:
            if end >= self.end or full_day and (end.date() == self.end.date()):
                days.append(TimePeriod(start, self.end))
                return days
            days.append(TimePeriod(start, end))
            start = end
            if full_day:
                start = start + timedelta(seconds=1)
            end = (start + timedelta(days=1)).replace(
                hour=00, minute=00, second=00, microsecond=00
            )
            if full_day:
                end = end - timedelta(seconds=1)
