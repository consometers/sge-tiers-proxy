import unittest
import os

from sgeproxy.publisher import RecordsByName, StreamsFiles
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
                print(record)

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
        print(as_xml)


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
