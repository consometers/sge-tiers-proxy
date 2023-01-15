from sgeproxy.metadata import (
    Metadata,
    Device,
    DeviceType,
    DeviceIdentifier,
    Measurement,
    MeasurementQuantity,
    MeasurementType,
    MeasurementDirection,
    MeasurementUnit,
    SamplingInterval,
)


class DeviceIdentifierEnedis(DeviceIdentifier):
    def __init__(self, prm: str):
        super().__init__(authority="enedis", type="prm", value=prm)


class MetadataEnedisConsumptionPowerActiveRaw(Metadata):
    def __init__(self, prm: str, sampling_interval: SamplingInterval):
        super().__init__(
            device=Device(
                type=DeviceType.ELECTRICITY_METER,
                identifier=DeviceIdentifierEnedis(prm),
            ),
            measurement=Measurement(
                name="active-power",
                quantity=MeasurementQuantity.POWER,
                type=MeasurementType.ELECTRICAL,
                direction=MeasurementDirection.CONSUMPTION,
                unit=MeasurementUnit.W,
                sampling_interval=sampling_interval,
            ),
        )


class MetadataEnedisConsumptionPowerCapacitiveRaw(Metadata):
    def __init__(self, prm: str, sampling_interval: SamplingInterval):
        super().__init__(
            device=Device(
                type=DeviceType.ELECTRICITY_METER,
                identifier=DeviceIdentifierEnedis(prm),
            ),
            measurement=Measurement(
                name="capacitive-power",
                quantity=MeasurementQuantity.POWER,
                type=MeasurementType.ELECTRICAL,
                direction=MeasurementDirection.CONSUMPTION,
                unit=MeasurementUnit.WR,
                sampling_interval=sampling_interval,
            ),
        )


class MetadataEnedisConsumptionPowerInductiveRaw(Metadata):
    def __init__(self, prm: str, sampling_interval: SamplingInterval):
        super().__init__(
            device=Device(
                type=DeviceType.ELECTRICITY_METER,
                identifier=DeviceIdentifierEnedis(prm),
            ),
            measurement=Measurement(
                name="inductive-power",
                quantity=MeasurementQuantity.POWER,
                type=MeasurementType.ELECTRICAL,
                direction=MeasurementDirection.CONSUMPTION,
                unit=MeasurementUnit.WR,
                sampling_interval=sampling_interval,
            ),
        )


class MetadataEnedisConsumptionVoltageRaw(Metadata):
    def __init__(self, prm: str, sampling_interval: SamplingInterval):
        super().__init__(
            device=Device(
                type=DeviceType.ELECTRICITY_METER,
                identifier=DeviceIdentifierEnedis(prm),
            ),
            measurement=Measurement(
                name="inductive-power",
                quantity=MeasurementQuantity.POWER,
                type=MeasurementType.ELECTRICAL,
                direction=MeasurementDirection.CONSUMPTION,
                unit=MeasurementUnit.V,
                sampling_interval=sampling_interval,
            ),
        )


class MetadataEnedisConsumptionPowerApparentMax(Metadata):

    MEASUREMENT = Measurement(
        name="apparent-power",
        quantity=MeasurementQuantity.POWER,
        type=MeasurementType.ELECTRICAL,
        direction=MeasurementDirection.CONSUMPTION,
        unit=MeasurementUnit.VA,
        sampling_interval=SamplingInterval("P1D"),
    )

    def __init__(self, prm: str):
        super().__init__(
            device=Device(
                type=DeviceType.ELECTRICITY_METER,
                identifier=DeviceIdentifierEnedis(prm),
            ),
            measurement=self.MEASUREMENT,
        )


class MetadataEnedisConsumptionEnergyActiveIndex(Metadata):

    MEASUREMENT = Measurement(
        name="active-energy-index",
        quantity=MeasurementQuantity.ENERGY,
        type=MeasurementType.ELECTRICAL,
        direction=MeasurementDirection.CONSUMPTION,
        unit=MeasurementUnit.WH,
        sampling_interval=SamplingInterval("P1D"),
    )

    def __init__(self, prm: str):
        super().__init__(
            device=Device(
                type=DeviceType.ELECTRICITY_METER,
                identifier=DeviceIdentifierEnedis(prm),
            ),
            measurement=self.MEASUREMENT,
        )
