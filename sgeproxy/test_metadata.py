import unittest
import datetime as dt
import typing
import dataclasses

from sgeproxy.metadata import SamplingInterval
from sgeproxy.metadata_enedis import MetadataEnedisConsumptionPowerActiveRaw


class TestMetadata(unittest.TestCase):

    # Internally Metadata is used to sort records with the same
    # metadata together, allowing group records by metadata and
    # send them all at once.

    def test_is_hashable(self):
        meta = MetadataEnedisConsumptionPowerActiveRaw(
            prm="09111642617347",
            sampling_interval=SamplingInterval(dt.timedelta(minutes=30)),
        )
        isinstance(meta, typing.Hashable)

    # Be safe and prevent modification of any instanciated metadata

    def test_is_frozen(self):
        meta = MetadataEnedisConsumptionPowerActiveRaw(
            prm="09111642617347",
            sampling_interval=SamplingInterval(dt.timedelta(minutes=30)),
        )
        with self.assertRaises(dataclasses.FrozenInstanceError):
            meta.device.identifier.value = "30001642617347"

    def can_be_used_to_group_records(self):
        pass


if __name__ == "__main__":

    unittest.main()
