import functools
import typing as t


class ExistingRanges:
    def __init__(self):
        self.existing_ranges: t.List[t.Tuple[int, int]] = list()

    def add(self, start: int, end: int):
        if (start, end) in self.existing_ranges:
            return

        if start > end:
            start, end = end, start

        self.existing_ranges.append((start, end))
        self.existing_ranges = self.merge(self.existing_ranges)

    @staticmethod
    def merge(existing_ranges: list):
        if len(existing_ranges) <= 1:
            return existing_ranges

        intervals = existing_ranges.copy()
        intervals.sort(key=lambda interval: interval[0])

        result = list((intervals.pop(0),))

        for start_i, end_i in intervals:
            edge_interval_start, edge_interval_end = result[-1]
            if start_i <= edge_interval_end + 1:
                result[-1] = edge_interval_start, max(edge_interval_end, end_i)
            else:
                result.append((start_i, end_i))

        return result

    def iter_partition(self, start: int, end: int):
        (*partitioners,) = (
            _
            for _ in functools.reduce(tuple.__add__, self.existing_ranges, ())
            if start < _ < end
        )

        if partitioners:
            yield from zip((start, *partitioners, end), (*partitioners, end))
        else:
            yield (start, end)

    def __contains__(self, other):
        (start, end) = other

        return any(
            starting <= start <= ending and starting <= end <= ending
            for starting, ending in self.existing_ranges
        )
