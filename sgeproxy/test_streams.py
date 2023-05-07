import unittest
import os

from sgeproxy.publisher import RecordsByName, StreamsFiles, StreamFiles
from sgeproxy.streams import R171, R50, R4x, Hdm
from sgeproxy.metadata import MeasurementDirection
from quoalise.data import Data, Metadata, Record
from slixmpp.xmlstream import ET
import datetime as dt

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
        elif series.startswith("consumption/power/capacitive/raw"):
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


class TestQuarantine(TestStreams):
    def test_quarantine(self):

        streams_files = StreamsFiles(
            inbox_dir=None,
            archive_dir=os.path.join(TEST_DATA_DIR, "quarantine"),
            errors_dir=None,
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
            publish_archives=True,
        )
        for f in streams_files.glob():
            for meta, record in streams_files.file_records(f):
                self.assert_valid_meta_and_record(meta, record)


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

    def test_provider_calendar(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "R171", "provider_calendar.zip"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        prms = set()
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            r171 = R171(data_file)
            for meta, record in r171.records():
                self.assert_valid_meta_and_record(meta, record)
                if "provider" in record.name:
                    prms.add(meta.device.identifier.value)
                # FIXME sum of indices for provider and distributor do not match
                # FIXME pmax is not given both for provider and distributor
        self.assertTrue(len(prms) > 0)
        # print(prms)


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

    def test_c5_cdc_empty_line(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C5_CDC_empty_line.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                self.assertFalse(list(hdm.records(is_c5=True)))

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

    def test_c5_idx(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C5_IDX.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                base_name = f"urn:dev:prm:{hdm.usage_point()}_consumption"

                some_expected_values = set()
                expected_names = set()
                for n, t, val in [
                    ("energy/active/index", "2020-03-31T00:00:00+02:00", 17657737),
                    (
                        "energy/active/index/distributor/base",
                        "2020-03-31T00:00:00+02:00",
                        17657737,
                    ),
                    (
                        "energy/active/index/provider/bchc",
                        "2020-03-31T00:00:00+02:00",
                        1600657,
                    ),
                    (
                        "energy/active/index/provider/bchp",
                        "2020-03-31T00:00:00+02:00",
                        3970026,
                    ),
                    (
                        "energy/active/index/provider/buhc",
                        "2020-03-31T00:00:00+02:00",
                        8513679,
                    ),
                    (
                        "energy/active/index/provider/buhp",
                        "2020-03-31T00:00:00+02:00",
                        21865270,
                    ),
                    (
                        "energy/active/index/provider/rhc",
                        "2020-03-31T00:00:00+02:00",
                        933661,
                    ),
                    (
                        "energy/active/index/provider/rhp",
                        "2020-03-31T00:00:00+02:00",
                        2213173,
                    ),
                    (
                        "energy/active/index/provider/hc",
                        "2023-01-25T23:00:00+01:00",
                        14228168,
                    ),
                    (
                        "energy/active/index/provider/hp",
                        "2023-01-25T23:00:00+01:00",
                        32744893,
                    ),
                    ("power/apparent/max", "2023-01-24T16:56:41+01:00", 4641),
                ]:
                    name = f"{base_name}/{n}"
                    time = dt.datetime.fromisoformat(t)
                    some_expected_values.add((name, time, val))
                    expected_names.add(name)

                for meta, record in hdm.records(is_c5=True):
                    self.assert_valid_meta_and_record(meta, record)
                    footprint = (record.name, record.time, record.value)
                    some_expected_values.discard(footprint)
                    self.assertTrue(record.name in expected_names)

                self.assertEqual(len(some_expected_values), 0)

    def test_c5_idx_empty(self):
        # File only contains headers with no values
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C5_IDX_empty.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            records = []
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                for meta, record in hdm.records(is_c5=True):
                    self.assert_valid_meta_and_record(meta, record)
                    records.append(record)
            self.assertEqual(len(records), 0)

    # TODO compare with values from a day stream
    # (are we computing total correctly?)

    # 2020-10-13T00:00:00+02:00;Arrêté quotidien;42595832;39139315;1763620;7177480;634973;4148978;;;;;95460198;;;;95460198 # noqa: E501
    # 2020-10-14T00:00:00+02:00;Arrêté quotidien;42782598;39139315;1763620;7177480;634973;4148978;;;;;95646964;;;;95646964 # noqa: E501
    # 2020-10-15T00:00:00+02:00;Arrêté quotidien;42968066;;;;;;;;;;95832432;;;;95832432 # noqa: E501
    # 2020-10-16T00:00:00+02:00;Arrêté quotidien;43169396;;;;;;;;;;96033762;;;;96033762 # noqa: E501
    def test_c5_idx_total_is_distributor(self):
        # File only contains headers with no values
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C5_IDX_total_is_distributor.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            records = []
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                for meta, record in hdm.records(is_c5=True):
                    self.assert_valid_meta_and_record(meta, record)
                    records.append(record)
            self.assertTrue(len(records) > 0)

    # Counter values can be found in data but calendar tell they should not be used
    #
    # 2022-02-27T23:00:00+01:00;Arrêté quotidien;10341184;29044825;;;;;;;;;40471015;;;;40471015 # noqa: E501
    # 2022-02-28T23:00:00+01:00;Arrêté quotidien;10359381;29104538;191045;735201;36448;122312;0;0;0;0;40548925;0;0;0;40548925 # noqa: E501
    # 2022-03-01T23:00:00+01:00;Arrêté quotidien;10374734;29155833;;;;;;;;;40615573;;;;40615573 # noqa: E501
    def test_c5_idx_unexpected_counters_values(self):
        # File only contains headers with no values
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C5_IDX_unexpected_counters_values.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            records = []
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                for meta, record in hdm.records(is_c5=True):
                    self.assert_valid_meta_and_record(meta, record)
                    records.append(record)
            self.assertTrue(len(records) > 0)

    def test_cdc_unexpected_sampling(self):
        """
        Sometimes midnight value is not given.
        In those cases 01:00 is skipped because it does not match a known
        sampling rate.
        TODO try to handle it better?
        2022-10-25T22:00:00+02:00;372
        2022-10-25T23:00:00+02:00;16
        2022-10-26T01:00:00+02:00;16
        2022-10-26T02:00:00+02:00;16
        """
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "CDC_unexpected_sampling.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            records = []
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                for meta, record in hdm.records(is_c5=True):
                    self.assert_valid_meta_and_record(meta, record)
                    records.append(record)
            self.assertTrue(len(records) > 0)

    def test_cdc_15min(self):
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "CDC_15min.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                found_15min = False
                for meta, record in hdm.records(is_c5=True):
                    self.assert_valid_meta_and_record(meta, record)
                    if meta.measurement.sampling_interval.value == "PT15M":
                        found_15min = True
            self.assertTrue(found_15min)

    def test_cdc_duplicates(self):
        """
        2022-07-04T14:00:00+02:00;64
        2022-07-04T14:00:00+02:00;64
        2022-07-04T14:00:00+02:00;64
        2022-07-04T14:00:00+02:00;64
        2022-07-04T14:30:00+02:00;62
        2022-07-04T14:30:00+02:00;62
        2022-07-04T14:30:00+02:00;62
        2022-07-04T14:30:00+02:00;62
        """
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "CDC_duplicates.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            records = []
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                for meta, record in hdm.records(is_c5=True):
                    self.assert_valid_meta_and_record(meta, record)
                    records.append(record)
            self.assertEqual(28, len(records))

    def test_c5_idx_provider_only(self):
        """
        Sometimes only the provider index is given
        2020-08-28T00:00:00+02:00;Arrêté quotidien;14904429;;;;;;;;;;;;;;14904429
        Thos lines are currently ignored since total is computed on distributor data
        """
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C5_IDX_provider_only.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                for meta, record in hdm.records(is_c5=True):
                    self.assert_valid_meta_and_record(meta, record)

    def test_c5_idx_no_calendar(self):
        """
        Dates but no values, calendar is omitted
        2023-01-23T23:00:00+01:00;Arrêté quotidien;;;;;;;;;;;;;;;
        2023-01-24T23:00:00+01:00;Arrêté quotidien;;;;;;;;;;;;;;;
        2023-01-25T23:00:00+01:00;Arrêté quotidien;;;;;;;;;;;;;;;
        Identifiant PRM;Type de donnees;Date de debut;Date de fin;Grandeur physique;Grandeur metier;Etape metier;Unite # noqa: E501
        00000000000000;Puissance maximale quotidienne;30/03/2020;25/01/2023;Puissance maximale atteinte;Consommation;; # noqa: E501
        Horodate;Valeur
        2020-03-30T00:00:00+02:00;
        2020-03-31T00:00:00+02:00;
        2020-04-01T00:00:00+02:00;
        """
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C5_IDX_no_calendar.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                self.assertFalse(list(hdm.records(is_c5=True)))

    def test_c5_idx_decreasing(self):
        """
        Counter decreased at a time not corresponding to calendar change
        2020-05-31T00:00:00+02:00;Arrêté quotidien;2513393;140442;;;;;;;;;1931591;533983;41339;146922;2653835 # noqa: E501
        2020-06-01T00:00:00+02:00;Arrêté quotidien;2513924;141456;;;;;;;;;1932122;534997;41339;146922;2655380 # noqa: E501
        2020-06-02T00:00:00+02:00;Arrêté quotidien;2514405;142501;;;;;;;;;1932603;536042;41339;0;2656906 # noqa: E501
        2020-06-03T00:00:00+02:00;Arrêté quotidien;2514950;143534;;;;;;;;;1933148;537075;41339;0;2658484 # noqa: E501
        2020-06-04T00:00:00+02:00;Arrêté quotidien;2515503;144599;;;;;;;;;1933701;538140;41339;0;2660102 # noqa: E501
        """
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "C5_IDX_decreasing.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            records = []
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                for meta, record in hdm.records(is_c5=True):
                    self.assert_valid_meta_and_record(meta, record)
                    records.append(record)
            self.assertTrue(len(records) > 0)

    def test_no_meta(self):
        """
        File just contains the first line
        Identifiant PRM;Type de donnees;Date de debut;Date de fin;Grandeur physique;Grandeur metier;Etape metier;Unite;Pas en minutes # noqa: E501
        """
        stream_files = StreamFiles(
            os.path.join(TEST_DATA_DIR, "HDM", "no_meta.csv"),
            aes_iv=Args.aes_iv,
            aes_key=Args.aes_key,
        )
        with stream_files as data_files:
            self.assertEqual(len(data_files), 1)
            data_file = data_files[0]
            with Hdm.open(data_file) as f:
                hdm = Hdm(f)
                self.assertFalse(list(hdm.records(is_c5=True)))


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
