from dataclasses import dataclass
from typing import Dict, Any
from enum import Enum


@dataclass(eq=True, frozen=True)
class DeviceIdentifier:
    authority: str
    type: str
    value: str

    def to_dict(self) -> Dict[str, Any]:
        return {"authority": self.authority, "type": self.type, "value": self.value}


class DeviceType(Enum):
    ELECTRICITY_METER = "electricity-meter"


@dataclass(eq=True, frozen=True)
class Device:
    type: DeviceType
    identifier: DeviceIdentifier

    def to_dict(self) -> Dict[str, Any]:
        return {"type": self.type.value, "identifier": self.identifier.to_dict()}


class MeasurementAggregation(Enum):
    MEAN = "mean"


@dataclass(init=True, eq=True, frozen=True)
class SamplingInterval:

    # ISO format like P1D or PT30M
    value: str


class MeasurementDirection(Enum):
    CONSUMPTION = "consumption"
    PRODUCTION = "production"


class MeasurementUnit(Enum):
    W = "W"
    VA = "VA"
    WH = "Wh"
    V = "V"
    WR = "Wr"  # TODO seems to be VAr in data files Wr in docs, should be VAr?


class MeasurementQuantity(Enum):
    POWER = "power"
    ENERGY = "energy"


class MeasurementType(Enum):
    ELECTRICAL = "electrical"


@dataclass(eq=True, frozen=True)
class Measurement:
    name: str
    direction: MeasurementDirection
    quantity: MeasurementQuantity
    type: MeasurementType
    unit: MeasurementUnit
    sampling_interval: SamplingInterval

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "direction": self.direction.value,
            "quantity": self.quantity.value,
            "type": self.type.value,
            "unit": self.unit.value,
            "sampling-interval": self.sampling_interval.value,
        }


# Currently used for the data we publish:
# - converting to xml is handled
# - parsing from xml is not handled


@dataclass(eq=True, frozen=True)
class Metadata:
    device: Device
    measurement: Measurement

    def to_dict(self) -> Dict[str, Any]:
        return {
            "device": self.device.to_dict(),
            "measurement": self.measurement.to_dict(),
        }
