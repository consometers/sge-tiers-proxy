import unittest
import os

from sgeproxy.publisher import RecordsByName, StreamsFiles, StreamFiles
from sgeproxy.streams import R171, R50, R4x, Hdm
from sgeproxy.metadata import MeasurementDirection
from quoalise.data import Data, Metadata, Record
from slixmpp.xmlstream import ET

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "test_data", "streams")


class Args:
    aes_iv = None
    aes_key = None


class TestStreams(unittest.TestCase):
    def assert_meta_matches_record(self, meta: Metadata, record: Record):
        # Id matches
        self.assertRegex(meta.device.identifier.value, r"^\d{14}$")
        self.assertTrue(
            record.name.startswith(f"urn:dev:prm:{meta.device.identifier.value}")
        )
        # Unit matches
        self.assertEqual(record.unit, meta.measurement.unit.value)

    def assert_valid_meta_and_record(self, meta: Metadata, record: Record):

        self.assert_meta_matches_record(meta, record)

        identifier, series = record.name.split("_")

        # TODO check calendar value like max/distributor/hph
        if series.startswith("consumption/energy/active/index"):
            self.assertEqual(record.unit, "Wh")
        elif series.startswith("consumption/power/active/raw"):
            self.assertEqual(record.unit, "W")
        elif series.startswith("consumption/power/inductive/raw"):
            self.assertEqual(record.unit, "Wr")
        elif series.startswith("consumption/power/active/max"):
            self.assertEqual(record.unit, "W")
        elif series.startswith("consumption/power/apparent/max"):
            self.assertEqual(record.unit, "VA")
        elif series.startswith("consumption/voltage/raw"):
            self.assertEqual(record.unit, "V")
        else:
            raise AssertionError(f"Unexpected series name {series}")

    def assert_load_curve(self, meta, record):
        self.assert_valid_meta_and_record(meta, record)
        self.assertTrue(record.name.endswith("consumption/power/active/raw"))


def day_records():
    streams_files = StreamsFiles(
        inbox_dir=None,
        archive_dir=os.path.join(TEST_DATA_DIR, "archive", "2023-01-15"),
        errors_dir=None,
        aes_iv=Args.aes_iv,
        aes_key=Args.aes_key,
        publish_archives=True,
    )
    records_by_name = RecordsByName()
    for f in streams_files.glob():
        for metadata, record in streams_files.file_records(f):
            records_by_name.add(metadata, record)
    return records_by_name


class TestDayStreams(TestStreams):
    def test_day_all(self):

        records_by_name = day_records()
        for metadata, records in records_by_name.get():
            for record in records:
                self.assert_valid_meta_and_record(metadata, record)

    def test_day_prefix(self):

        NAME = "urn:dev:prm:30001444954220_consumption/power/inductive/raw"

        records_by_name = day_records()

        for _, records in records_by_name.get(NAME):
            for record in records:
                self.assertEqual(NAME, record.name)

    def test_day_chunk(self):

        NAME = "urn:dev:prm:30001444954220_consumption/power/inductive/raw"
        CHUNK_SIZE = 10

        all_records_chunks = []

        records_by_name = day_records()

        for metadata, records in records_by_name.get(NAME):
            all_records = records
            all_records_meta = metadata

        for metadata, records in records_by_name.get(NAME, chunk_size=CHUNK_SIZE):
            self.assertEqual(metadata, all_records_meta)
            self.assertTrue(len(records) <= CHUNK_SIZE)
            all_records_chunks.extend(records)

        self.assertEqual(all_records, all_records_chunks)

    def test_day_to_xml(self):

        records_by_name = day_records()
        for metadata, records in records_by_name.get():
            data = Data(metadata=Metadata(metadata.to_dict()), records=records)
            as_xml = data.to_xml()
            as_xml = ET.tostring(as_xml, encoding="utf8", method="xml")
            print(as_xml.decode("utf-8"))

    def test_get_records_grouped_by_metadata(self):

        NAME = "urn:dev:prm:30001444954220_consumption/energy/active/index"
        # This series name will match with records named
        #
        # …_consumption/energy/active/index
        # …_consumption/energy/active/index/distributor/hph
        # …_consumption/energy/active/index/distributor/hch
        # …
        #
        # We them all at once, as they share the same metadata

        records_by_name = day_records()
        records_by_meta = list(records_by_name.get(NAME))
        self.assertTrue(len(records_by_meta) == 1)
        self.assertTrue(len(records_by_meta[0][1]) > 0)


class TestR4x(TestStreams):

    # Around 2023-03-20, sampling rate was changed to 5 min on some points
    def test_5min(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "R4x", "5min.zip"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            r4x = R4x(data_file)
            records_5min = 0
            for meta, record in r4x.records():
                self.assert_valid_meta_and_record(meta, record)
                if meta.measurement.sampling_interval.value == "PT5M":
                    records_5min += 1
            self.assertTrue(records_5min > 0)

    # Sometimes the GrandeurMetier tag is not present, assuming CONS
    def test_empty_direction(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "R4x", "empty_Grandeur_Metier.zip"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            for data_file in data_files:
                r4x = R4x(data_file)
                for meta, record in r4x.records():
                    self.assert_valid_meta_and_record(meta, record)
                    self.assertEqual(
                        meta.measurement.direction, MeasurementDirection.CONSUMPTION
                    )

    def test_missing_values(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "R4x", "missing_values.zip"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            for data_file in data_files:
                r4x = R4x(data_file)
                for meta, record in r4x.records():
                    self.assert_valid_meta_and_record(meta, record)


class TestR50(TestStreams):
    def test_periods_stamped_at_the_begining(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "R50", "missing_values.zip"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            r50 = R50(data_file)
            _, record = next(r50.records())
            self.assertEqual(record.time.isoformat(), "2023-01-26T00:00:00+01:00")

    def test_missing_values(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "R50", "missing_values.zip"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            for data_file in data_files:
                r50 = R50(data_file)
                for meta, record in r50.records():
                    self.assert_valid_meta_and_record(meta, record)


class TestR171(TestStreams):

    # Some streams uses active W for pmax power, most of the others apparent VA
    def test_pmax_true_power(self):
        stream_files = StreamFiles(
            os.path.join(
                TEST_DATA_DIR, "R171", "pmax_active_power_and_missing_values.zip"
            ),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )

        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            r171 = R171(data_file)
            prms_pmax_active = set()
            prms_pmax_apparent = set()
            for meta, record in r171.records():
                self.assert_valid_meta_and_record(meta, record)
                if "power/active/max" in record.name:
                    prms_pmax_active.add(meta.device.identifier.value)
                elif "power/apparent/max" in record.name:
                    prms_pmax_apparent.add(meta.device.identifier.value)
            self.assertTrue(len(prms_pmax_active) > 0)
            self.assertTrue(len(prms_pmax_apparent) > 0)
            # It seems to be either one or the other, not both
            self.assertTrue(len(prms_pmax_active.intersection(prms_pmax_apparent)) == 0)


class TestHdm(TestStreams):
    def test_c5_cdc(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C5_CDC.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                records = []
                for meta, record in hdm.records(is_c5=True):
                    self.assert_load_curve(meta, record)
                    self.assertEqual(meta.measurement.sampling_interval.value, "PT30M")
                    records.append(record)
                self.assertEqual(6050, len(records))
                # First record is from 00:00 to 00:30
                self.assertEqual(
                    records[0].time.isoformat(), "2021-09-01T00:00:00+02:00"
                )
                self.assertEqual(records[0].value, 394)
                # First record is from 23:30 to 00:00
                self.assertEqual(
                    records[-1].time.isoformat(), "2022-01-04T23:30:00+01:00"
                )
                self.assertEqual(records[-1].value, 436)

    def test_c5_cdc_missing_values(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C5_CDC_missing_values.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        # Some values are missing with the date specified
        #   2021-01-07T22:00:00+01:00;2491
        #   2021-01-07T23:00:00+01:00;
        #   2021-01-08T00:00:00+01:00;749
        #   No sampling interval guess issue here
        # Some values are missing without the date being specified (no 23:00)
        #   2021-02-06T22:00:00+01:00;2168
        #   2021-02-07T00:00:00+01:00;358
        #   For now in this case 2021-02-07T00:00 is discarded because sampling
        #   interval is hard to guess.
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            expected_sampling_interval = "PT60M"
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                records = []
                for meta, record in hdm.records(is_c5=True):
                    self.assert_load_curve(meta, record)
                    # Sampling interval changed in this file
                    if (
                        expected_sampling_interval == "PT60M"
                        and meta.measurement.sampling_interval.value == "PT30M"
                    ):
                        expected_sampling_interval = "PT30M"
                    else:
                        self.assertEqual(
                            expected_sampling_interval,
                            meta.measurement.sampling_interval.value,
                        )
                    records.append(record)
                # First record is first non missing value
                self.assertEqual(
                    records[0].time.isoformat(), "2021-01-07T13:00:00+01:00"
                )

    def test_c4_cdc(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C4_CDC.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            records = []
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                for meta, record in hdm.records(is_c5=False):
                    self.assert_load_curve(meta, record)
                    self.assertEqual(meta.measurement.sampling_interval.value, "PT10M")
                    records.append(record)
                # First record at 00:00
                self.assertEqual(
                    records[0].time.isoformat(), "2020-02-03T00:00:00+01:00"
                )


if __name__ == "__main__":

    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "aes_iv",
        help="iv used to decrypt stream files",
    )
    parser.add_argument(
        "aes_key",
        help="key used to decrypt stream files",
    )
    args, unittest_args = parser.parse_known_args()

    Args.aes_iv = args.aes_iv
    Args.aes_key = args.aes_key

    unittest.main(argv=[sys.argv[0]] + unittest_args)
