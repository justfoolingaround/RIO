import bisect
import functools
import logging
import time
import typing as t


class ExistingRanges:
    def __init__(self):
        self.existing_ranges: t.List[t.Tuple[int, int]] = list()

    def add(self, start: int, end: int):
        if (start, end) not in self.existing_ranges:
            bisect.insort_right(self.existing_ranges, (start, end))

            index = 0

            for start, end in self.existing_ranges[1:]:
                if self.existing_ranges[index][1] >= start:
                    self.existing_ranges[index] = (
                        self.existing_ranges[index][0],
                        max(self.existing_ranges[index][1], end),
                    )
                else:
                    index += 1
                    self.existing_ranges[index] = (start, end)

            self.existing_ranges = self.existing_ranges[: index + 1]

    def iter_partition(self, start: int, end: int):
        partitioners = [
            partition
            for partition in functools.reduce(tuple.__add__, self.existing_ranges, ())
            if start < partition < end
        ]

        if partitioners:
            yield from zip([start, *partitioners, end], [*partitioners, end])
        else:
            yield (start, end)

    def __contains__(self, other):
        start, end = other

        return any(
            starting <= start <= ending and starting <= end <= ending
            for starting, ending in self.existing_ranges
        )


def fmt_function_call(name, args, kwargs):
    if kwargs:
        fmt_kwargs = {
            ", ".join([f"{key}={repr(value)}" for key, value in kwargs.items()])
        }
    else:
        fmt_kwargs = ""

    if args:
        fmt_args = ", ".join([repr(arg) for arg in args])

    else:
        fmt_args = ""

    if fmt_args and fmt_kwargs:
        fmt_args += ", "

    return f"{name}({fmt_args}{fmt_kwargs})"


def log_member_function_activity(activity_name=None, nano_precision=False):
    time_func = time.perf_counter_ns if nano_precision else time.perf_counter

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if hasattr(self, "logger"):
                logger = self.logger
            else:
                logger = logging.getLogger("activity_logger")

            start = time_func()
            result = func(self, *args, **kwargs)

            name = f"{self.__class__.__name__}.{activity_name or func.__name__}"

            logger.info(
                f"{fmt_function_call(name, args, kwargs)} took {time_func() - start} {'nano' if nano_precision else ''}seconds"
            )
            return result

        return wrapper

    return decorator
