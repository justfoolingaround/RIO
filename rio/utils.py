class ExistingRanges:
    def __init__(self):
        self.existing_ranges = set()

    def add(self, start, end):
        self.existing_ranges.add((start, end))
        self.merge()

    def merge(self):
        if len(self.existing_ranges) in (0, 1):
            return

        intervals = list(self.existing_ranges)
        intervals.sort(key=lambda interval: interval[0])

        result = list((intervals.pop(0),))

        for start_i, end_i in intervals:
            edge_interval_start, edge_interval_end = result[-1]
            if start_i <= edge_interval_end:
                result[-1] = edge_interval_start, max(edge_interval_end, end_i)
            else:
                result.append((start_i, end_i))

        self.existing_ranges = set(result)

    def iter_partition(self, start, end):

        if not self.existing_ranges:
            yield (start, end)
            return

        for element_start, element_end in self.existing_ranges.copy():
            if start < element_start:
                yield ((start, element_start))

            if element_start < end:
                yield ((element_start, min(end, element_end)))

            if element_end < end:
                yield ((element_end, end))

    def __contains__(self, other):
        (start, end) = other
        return any(
            starting <= start < ending and starting <= end < ending
            for starting, ending in self.existing_ranges
        )
