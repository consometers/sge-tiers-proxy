#!/usr/bin/env python3

import asyncio
import logging
import os
import glob
import tempfile
import shutil
import subprocess
import datetime as dt
import re
import time
from typing import Dict, List, Tuple, Iterable

import slixmpp
from slixmpp.xmlstream import ET
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import sgeproxy.sge
import sgeproxy.xmpp_interface
import sgeproxy.streams
from sgeproxy.db import Subscription, UsagePoint, UsagePointSegment
from quoalise.data import Data, Record, Metadata

# SgeProxyMeta will be merged with quoalise.data.Metadata at some point
from sgeproxy.metadata import Metadata as SgeProxyMeta


class XmppPublisher(slixmpp.ClientXMPP):
    def __init__(self, conf):
        self.conf = conf
        slixmpp.ClientXMPP.__init__(
            self,
            conf["full_jid"] + "-publisher",
            conf["password"],
        )
        self.add_event_handler("session_start", self.start)
        self.session_started = asyncio.Future()

    def connect(self):
        if "address" in self.conf and "port" in self.conf:
            slixmpp.ClientXMPP.connect(
                self, address=(self.conf["address"], self.conf["port"])
            )
        else:
            slixmpp.ClientXMPP.connect(self)

    async def start(self, event):
        # negative priority prevents to receive messages that are not
        # explicitely addressed to this resource
        self.send_presence(ppriority=-1)
        # await self.get_roster()

        # await self.send_custom_iq()
        self.session_started.set_result(True)

    async def send_data(self, to, metadata, records):

        # Quick and dirty rate limit
        await asyncio.sleep(0.5)

        message = self.Message()
        message["from"] = self.boundjid.full
        message["to"] = to

        quoalise = ET.Element("{urn:quoalise:0}quoalise")
        message.append(quoalise)

        data = Data(metadata=Metadata(metadata.to_dict()), records=records)
        quoalise.append(data.to_xml())

        return message.send()


class CorruptedFile(Exception):
    pass


class StreamsFiles:
    def __init__(
        self,
        inbox_dir,
        archive_dir,
        errors_dir,
        keys,
        publish_archives=False,
    ):
        self.inbox_dir = inbox_dir
        self.archive_dir = archive_dir
        self.errors_dir = errors_dir
        self.keys = keys
        self.publish_archives = publish_archives

    def open(self, file_path, aes_iv, aes_key):
        return StreamFiles(file_path, aes_iv, aes_key)

    def archive(self, file_path):
        if self.publish_archives:
            # We are publishing data that have archived already
            return
        archive_file_path = os.path.join(
            self.archive_dir,
            dt.date.today().isoformat(),
            os.path.basename(file_path),
        )
        os.makedirs(os.path.dirname(archive_file_path), exist_ok=True)
        os.rename(file_path, archive_file_path)

    def move_to_errors(self, file_path):
        if self.publish_archives:
            # Do not mess with archived files
            return
        error_file_path = os.path.join(
            self.errors_dir,
            dt.date.today().isoformat(),
            os.path.basename(file_path),
        )
        os.makedirs(os.path.dirname(error_file_path), exist_ok=True)
        os.rename(file_path, error_file_path)

    def glob(self):
        if self.publish_archives:
            pattern = os.path.join(self.archive_dir, "**", "*")
        else:
            pattern = os.path.join(self.inbox_dir, "**", "*")
        return glob.iglob(pattern, recursive=True)

    def file_records(self, path, is_prm_c5=lambda prm: True):

        filename = os.path.basename(path)

        if os.path.isdir(path):
            return

        # We do nothing with stream transfer companion metadata for now
        if path.endswith("_svc.xml"):
            return

        filename_stream_patterns = {
            r"^ENEDIS_R171_.+\.zip$": sgeproxy.streams.R171,
            r"^ERDF_R151_.+\.zip$": sgeproxy.streams.R151,
            r"^ERDF_R50_.+\.zip$": sgeproxy.streams.R50,
            r"^ENEDIS_.+_R4Q_CDC_.+\.zip$": sgeproxy.streams.R4x,
            r"^Enedis_SGE_HDM.+\.csv$": sgeproxy.streams.Hdm,
        }

        stream_handler = None
        for pattern in filename_stream_patterns:
            if re.match(pattern, filename):
                stream_handler = filename_stream_patterns[pattern]
                break

        if stream_handler is None:
            logging.error(f"No handler for file {path}")
            return

        for key in self.keys:
            try:
                with self.open(path, key["aes_iv"], key["aes_key"]) as data_files:
                    for data_file in data_files:
                        # FIXME(cyril) history is published like subscriptions for now
                        if stream_handler == sgeproxy.streams.Hdm:
                            with sgeproxy.streams.Hdm.open(data_file) as io:
                                stream = stream_handler(io)
                                # FIXME(cyril) depending on db is ugly here
                                for metadata, record in stream.records(
                                    is_c5=is_prm_c5(stream.usage_point())
                                ):
                                    yield metadata, record
                        else:
                            stream = stream_handler(data_file)
                            for metadata, record in stream.records():
                                yield metadata, record  # or yield stream.records()?
                return
            except CorruptedFile:
                # Try with the following key
                continue
        raise CorruptedFile(f"Unable to decrypt {path}")


class StreamFiles:
    def __init__(self, inbox_file_path, aes_iv, aes_key):

        self.inbox_file_path = inbox_file_path
        self.aes_iv = aes_iv
        self.aes_key = aes_key

    def __enter__(self):

        self.temp_dir = tempfile.mkdtemp()

        filename = os.path.basename(self.inbox_file_path)
        temp_file = os.path.join(self.temp_dir, filename)

        try:
            subprocess.run(
                [
                    "openssl",
                    "enc",
                    "-d",
                    "-aes-128-cbc",
                    "-K",
                    self.aes_key,
                    "-iv",
                    self.aes_iv,
                    "-in",
                    self.inbox_file_path,
                    "-out",
                    temp_file,
                ],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise CorruptedFile("openssl failed") from e

        # If unencrypted file is a zip archive, extract it
        if temp_file.endswith(".zip"):
            try:
                subprocess.run(["unzip", temp_file, "-d", self.temp_dir], check=True)
            except subprocess.CalledProcessError as e:
                raise CorruptedFile("unzip failed") from e
            os.remove(temp_file)

        files = []
        for path, dirs, fs in os.walk(self.temp_dir):
            fs = [os.path.join(path, f) for f in fs]
            files.extend(fs)

        return files

    def __exit__(self, type, value, traceback):
        shutil.rmtree(self.temp_dir)


def chunks(lst: List[Record], n) -> Iterable[List[Record]]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


class RecordsByName:
    def __init__(self):
        self.records: Dict[str, Dict[SgeProxyMeta, List[Record]]] = {}

    def add(self, metadata: SgeProxyMeta, record: Record) -> None:
        if record.name not in self.records:
            self.records[record.name] = {}
        if metadata not in self.records[record.name]:
            self.records[record.name][metadata] = []
        self.records[record.name][metadata].append(record)

    def get(
        self, prefix: str = "", chunk_size: int = 0
    ) -> Iterable[Tuple[SgeProxyMeta, List[Record]]]:
        # Group records by metadata to be sent together
        all_records_by_meta: Dict[SgeProxyMeta, List[Record]] = {}
        for name, records_by_meta in self.records.items():
            for meta, records in records_by_meta.items():
                if name.startswith(prefix):
                    if meta not in all_records_by_meta:
                        all_records_by_meta[meta] = records.copy()
                    else:
                        all_records_by_meta[meta].extend(records)
        for meta, records in all_records_by_meta.items():
            if chunk_size:
                for records_chunk in chunks(records, chunk_size):
                    yield meta, records_chunk
            else:
                yield meta, records

    def count(self):
        value = 0
        for records_by_meta in self.records.values():
            for records in records_by_meta.values():
                value += len(records)
        return value


class Throttle:
    def __init__(self, record_rate):
        self.record_rate = record_rate
        self.begin = time.monotonic()
        self.count = 0

    def update(self, records_sent):
        self.count += records_sent
        throttled_duration = self.count / self.record_rate
        actual_duration = time.monotonic() - self.begin
        sleep_needed = throttled_duration - actual_duration
        if sleep_needed > 0:
            logging.info(f"Throttling, sleep {sleep_needed:.2f} s")
            time.sleep(sleep_needed)


if __name__ == "__main__":

    import argparse
    import sgeproxy.config
    import logging.handlers

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "conf", help="Configuration file (typically private/*.conf.json)"
    )
    parser.add_argument(
        "--publish-archives", help="Send archives instead of inbox", action="store_true"
    )
    parser.add_argument("--user", help="Only send data to the specified user")
    parser.add_argument(
        "--filter", help="substring that should be found in records ids to keep"
    )
    args = parser.parse_args()

    if args.publish_archives and not args.user:
        raise RuntimeError("Please provide a single user when publishing archives")

    conf = sgeproxy.config.File(args.conf)

    logger = logging.getLogger()
    # Log info to a file, ignoring the log level from command line
    # (rotates every monday, can be deleted regularly)
    os.makedirs(conf["logs_dir"], exist_ok=True)
    debug_log = logging.handlers.TimedRotatingFileHandler(
        os.path.join(conf["logs_dir"], "publisher.debug.log"),
        when="W0",
        interval=1,
        backupCount=0,
        atTime=dt.datetime.min,
    )
    debug_log.setLevel(logging.DEBUG)
    debug_log.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(debug_log)

    debug = []
    debug.append("slixmpp")

    for logger_name in debug:
        module_logger = logging.getLogger(logger_name)
        module_logger.addHandler(debug_log)
        module_logger.setLevel(logging.DEBUG)
        # logger.propagate = True

    # Log error to a file, ignoring the log level from command line
    # (rotates every monday, should be archived)
    error_log = logging.handlers.TimedRotatingFileHandler(
        os.path.join(conf["logs_dir"], "publisher.error.log"),
        when="W0",
        interval=1,
        backupCount=0,
        atTime=dt.datetime.min,
    )
    error_log.setLevel(logging.ERROR)
    error_log.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(error_log)

    db_engine = create_engine(conf["db"]["url"])
    db_session_maker = sessionmaker(bind=db_engine)
    db_session = db_session_maker()

    xmpp = XmppPublisher(conf["xmpp"])

    # loop = asyncio.get_event_loop()

    # loop.run_until_complete(main(conf, sge_credentials))
    # loop.run_forever()

    streams_files = StreamsFiles(
        conf["streams"]["inbox_dir"],
        conf["streams"]["archive_dir"],
        conf["streams"]["errors_dir"],
        conf["streams"]["keys"],
        publish_archives=args.publish_archives,
    )

    xmpp.connect()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait_for(xmpp.session_started, 10))

    files = list(streams_files.glob())

    def is_prm_c5(prm):
        return db_session.query(UsagePoint).get(prm).segment == UsagePointSegment.C5

    GROUP_FILES = 10

    throttle = Throttle(100)

    while files:
        records_by_name = RecordsByName()
        files_group: List[str] = []
        # TODO add a datapoint limit?
        while files and len(files_group) < GROUP_FILES:
            f = files.pop(0)
            if os.path.isdir(f):
                continue
            logging.info(f"Parsing {f}")
            try:
                file_records_count = 0
                for metadata, record in streams_files.file_records(f, is_prm_c5):
                    if not args.filter or args.filter in record.name:
                        records_by_name.add(metadata, record)
                        file_records_count += 1
                if file_records_count > 0:
                    files_group.append(f)
                else:
                    streams_files.archive(f)
            except Exception:
                logging.exception(f"Unable to parse data from {f}")
                streams_files.move_to_errors(f)
        if records_by_name.count() != 0:
            for sub in db_session.query(Subscription).all():
                # TODO move check to query
                if args.user and args.user != sub.user_id:
                    continue
                if sub.user_id == "cyril@lugan.fr":
                    # currently having a consent issue
                    continue

                series_name = f"urn:dev:prm:{sub.usage_point_id}_{sub.series_name}"

                for metadata, records in records_by_name.get(
                    prefix=series_name, chunk_size=1000
                ):
                    with sub.notification_checks():
                        loop.run_until_complete(
                            xmpp.send_data(sub.user_id, metadata, records)
                        )
                        throttle.update(len(records))
        # Files have been sent, archive them
        for f in files_group:
            streams_files.archive(f)

    xmpp.disconnect()
