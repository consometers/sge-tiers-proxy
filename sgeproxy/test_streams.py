import unittest
import os

from sgeproxy.publisher import RecordsByName, StreamsFiles, StreamFiles
from sgeproxy.streams import Hdm
from sgeproxy.metadata import MeasurementUnit
from quoalise.data import Data, Metadata
from slixmpp.xmlstream import ET

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "test_data", "streams")


class Args:
    aes_iv = None
    aes_key = None


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


class TestStreams(unittest.TestCase):
    def test_day_all(self):

        records_by_name = day_records()
        for metadata, records in records_by_name.get():
            print(metadata.to_dict())
            for record in records:
                self.assertEqual(record.unit, metadata.measurement.unit.value)
                self.assertTrue(
                    record.name.startswith(
                        f"urn:dev:prm:{metadata.device.identifier.value}"
                    )
                )

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


class TestHdm(unittest.TestCase):
    def assert_load_curve(self, meta, record):
        self.assertEqual(meta.measurement.unit, MeasurementUnit.W)
        self.assertRegex(meta.device.identifier.value, r"^\d{14}$")

        self.assertEqual(record.unit, meta.measurement.unit.value)
        self.assertEqual(
            record.name,
            f"urn:dev:prm:{meta.device.identifier.value}_consumption/power/active/raw",
        )

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
                for meta, record in hdm.records():
                    self.assert_load_curve(meta, record)
                    records.append(record)
                self.assertEqual(6050, len(records))
                self.assertEqual(
                    records[0].time.isoformat(), "2021-09-01T00:30:00+02:00"
                )
                self.assertEqual(records[0].value, 394)
                self.assertEqual(
                    records[-1].time.isoformat(), "2022-01-05T00:00:00+01:00"
                )
                self.assertEqual(records[-1].value, 436)

    def test_c5_cdc_missing_values(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C5_CDC_missing_values.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                records = []
                for meta, record in hdm.records():
                    self.assert_load_curve(meta, record)
                    records.append(record)
                # First record is first non missing value
                self.assertEqual(
                    records[0].time.isoformat(), "2021-01-07T14:00:00+01:00"
                )
                self.assertEqual(records[0].value, 1061)

    def test_c4_cdc(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C4_CDC.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                for meta, record in hdm.records():
                    self.assert_load_curve(meta, record)


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
