"""What a catalog channel is to its device: its state, an order sent to it, or a reading.

The house names every channel ``Function.Device.Datapoint``. A ``-Status``
datapoint is the state of its device; a datapoint it reports on (the same
datapoint without the suffix, or one that extends it with a dash:
``Dimmen-Absolut`` for ``Dimmen-Status``) is a command; everything else is a
reading. Which datapoints a device has comes from the catalog, so a lone
``Dimmen-Absolut`` still knows it is a command.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

Role = Literal["status", "command", "reading"]

_STATUS_SUFFIX = "-Status"
_ANOMALY_SUFFIX = "-Anomalie"


def device_of(name: str) -> str:
    """The device part of a catalog name: everything before the datapoint."""
    return name.rpartition(".")[0]


def classify(names: Iterable[str], catalog_names: Iterable[str]) -> dict[str, Role]:
    """The role of each name, judged against the datapoints its device has in the catalog."""
    datapoints: dict[str, set[str]] = {}
    for name in catalog_names:
        device, _, datapoint = name.rpartition(".")
        datapoints.setdefault(device, set()).add(datapoint)

    roles: dict[str, Role] = {}
    for name in names:
        device, _, datapoint = name.rpartition(".")
        bases = [
            point.removesuffix(_STATUS_SUFFIX)
            for point in datapoints.get(device, ())
            if point.endswith(_STATUS_SUFFIX)
        ]
        if datapoint.endswith(_STATUS_SUFFIX):
            roles[name] = "status"
        elif datapoint.endswith(_ANOMALY_SUFFIX):
            roles[name] = "reading"
        elif any(datapoint == base or datapoint.startswith(base + "-") for base in bases):
            roles[name] = "command"
        else:
            roles[name] = "reading"
    return roles
