"""Miscellaneous utility functions."""

from datetime import datetime, timedelta
from typing import Any, Callable, Iterable, Iterator, Protocol, TypeVar


T = TypeVar("T")
G = TypeVar("G")
T_contra = TypeVar("T_contra", contravariant=True)


class Addable(Protocol[T]):
    def __add__(self, other: T, /) -> T: ...


A = TypeVar("A", bound=Addable[Any])


class Comparable(Protocol[T_contra]):
    def __lt__(self, other: T_contra, /) -> bool: ...


I = TypeVar("I", bound=Comparable[Any])


def datetimeRangeDay(start: datetime, end: datetime) -> Iterator[datetime]:
    """Returns all datetimes from `start` (inclusive) to `end` (exclusive) on a 1-day interval."""

    for i in range((end - start).days):
        yield start + timedelta(days=i)


def fill(
    items: Iterable[T],
    indices: Iterable[I],
    indexBy: Callable[[T], I],
    groupBy: Callable[[T], G],
    fillWith: Callable[[I, G, T], T],
) -> Iterator[T]:
    """
    Given current `items` and expected `indices`, maps items to indices by `indexBy`, groups items by `groupBy`, and ensures that each group has at least 1 item per expected index.
    Missing items are filled in by `fillWith` provided `(index, group, nearestItem)`.
    Items are returned in no particular order.
    """

    indices = list(indices)

    groups: dict[G, list[T]] = {}
    for item in items:
        groups.setdefault(groupBy(item), []).append(item)

    for groupKey, group in groups.items():
        group.sort(key=indexBy)

        end = len(group)
        nearest = group[0]
        j = 0
        for i in indices:
            # already have a matching item, save current prev and continue
            if i == indexBy(group[j]):
                nearest = group[j]
                j = min(j + 1, end - 1)
            else:
                # append missing to the end
                group.append(fillWith(i, groupKey, nearest))

        yield from group


def cumulativeSum(
    items: Iterable[T], key: Callable[[T], G], value: Callable[[T], A], default: A
) -> Iterator[tuple[T, A]]:
    """Returns a running total of `items` summed by `value`s of like-`key`d previous items."""

    totals: dict[G, A] = {}

    for item in items:
        k = key(item)
        total = totals.get(k, default) + value(item)
        totals[k] = total

        yield (item, total)
