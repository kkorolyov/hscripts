import json
from collections import defaultdict
from contextlib import contextmanager
from datetime import date
from time import mktime, sleep
from typing import Any, Iterable, Protocol, TypeVar

import requests

from hmetrics.util import partition

S = TypeVar("S", bound=str)
T = TypeVar("T", covariant=True)


class Numeric(Protocol[T]):
    def __round__(self, digits: int, /) -> T: ...
    def __float__(self, /) -> float: ...


N = TypeVar("N", bound=Numeric[Any])


@contextmanager
def client(url: str):
    """Returns a metrics client sending metrics to `url`."""
    res = Prometheus(url)
    try:
        yield res
    finally:
        res.flush()


class Prometheus:
    """Pushes metrics to Prometheus."""

    _url: str
    _buffer = list[str]()

    def __init__(self, url: str) -> None:
        self._url = url

    def get(self, pattern: str) -> Iterable[str]:
        return []

    def delete(self, pattern: str):
        """Deletes samples for timeseries matching name `pattern`."""

        requests.post(
            f"{self._url}/api/v1/admin/tsdb/delete_series",
            {"match[]": f'{{__name__=~"{pattern}"}}'},
        ).raise_for_status()
        requests.post(
            f"{self._url}/api/v1/admin/tsdb/clean_tombstones"
        ).raise_for_status()

        print(f"deleted metrics matching {pattern} at {self._url}")

    def push(self, name: str, labels: dict[S, str], samples: dict[date, N]):
        """Buffers `samples` for a timeseries of `name` and `labels` to send as part of the next `flush()`."""

        labelsStr = ",".join((f'{k}="{v}"' for k, v in labels.items()))
        for k, v in sorted(samples.items(), key=lambda t: t[0]):
            self._buffer.append(
                f"{name}{{{labelsStr}}} {round(v, 2)} {int(mktime(k.timetuple()))}"
            )

    def flush(self):
        with open("om.txt", "w") as f:
            f.write("\n".join(self._buffer))
            f.write("\n# EOF\n")


class VmClient:
    """Pushes metrics to VictoriaMetrics at a given url."""

    _url: str
    _timeBucketDays = 30
    _buffer = list[str]()

    def __init__(self, url: str) -> None:
        self._url = url

    def get(self, pattern: str) -> Iterable[str]:
        """Returns samples for timeseries matching name `pattern`."""

        for line in requests.post(
            f"{self._url}/api/v1/export",
            {"match[]": f'{{__name__=~"{pattern}"}}'},
            stream=True,
        ).iter_lines():
            yield line

    def delete(self, pattern: str):
        """Deletes samples for timeseries matching name `pattern`."""

        requests.post(
            f"{self._url}/api/v1/admin/tsdb/delete_series",
            {"match[]": f'{{__name__=~"{pattern}"}}'},
        ).raise_for_status()

        print(f"deleted metrics matching {pattern} at {self._url}")

    def push(self, name: str, labels: dict[S, str], samples: dict[date, N]) -> None:
        """Buffers `samples` for a timeseries of `name` and `labels` to send as part of the next `flush()`."""

        # ensure individual line items are within timeBucket
        offset = min(samples.keys())
        bucketedSamples = defaultdict[int, list[tuple[date, N]]](list)
        for k, v in samples.items():
            bucketedSamples[(k - offset).days // self._timeBucketDays].append((k, v))

        for bucket in bucketedSamples.values():
            self._buffer.append(
                json.dumps(
                    {
                        "metric": {"__name__": name, **labels},
                        "values": [float(round(v, 2)) for _, v in bucket],
                        "timestamps": [
                            int(mktime(k.timetuple()) * 10**3) for k, _ in bucket
                        ],
                    }
                )
            )

        print(
            f"buffered {len(samples)} samples for {name}{{{labels}}} across {len(bucketedSamples.keys())} buckets"
        )

    def flush(self):
        """Sends buffered metrics to the backing url."""

        batchSize = 5000
        print(
            f"flushing {len([value for line in self._buffer for value in json.loads(line)['values']])} values over {len(self._buffer)} lines in {batchSize}-line batches to {self._url}..."
        )

        for lines in partition(self._buffer, batchSize):
            requests.post(
                f"{self._url}/api/v1/import",
                "\n".join(lines).encode(),
                stream=True,
                timeout=300,
            ).raise_for_status()

            print("flushed a batch")
            sleep(10)

        print(f"flushed {len(self._buffer)} lines to {self._url}")

        requests.post(f"{self._url}/internal/resetRollupResultCache").raise_for_status()
