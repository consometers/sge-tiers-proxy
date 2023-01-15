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
from typing import Dict, List, Tuple, Iterable

import slixmpp
from slixmpp.xmlstream import ET
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import sgeproxy.sge
import sgeproxy.xmpp_interface
import sgeproxy.streams
from sgeproxy.db import Subscription
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


class StreamsFiles:
    def __init__(
        self,
        inbox_dir,
        archive_dir,
        errors_dir,
        aes_iv,
        aes_key,
        publish_archives=False,
    ):
        self.inbox_dir = inbox_dir
        self.archive_dir = archive_dir
        self.errors_dir = errors_dir
        self.aes_iv = aes_iv
        self.aes_key = aes_key
        self.publish_archives = publish_archives

    def open(self, file_path):
        return StreamFiles(file_path, self.aes_iv, self.aes_key)

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

    def file_records(self, path):

        filename = os.path.basename(path)

        # We do nothing with stream transfer companion metadata for now
        if re.match(r"_svc.xml$", path):
            self.archive(path)

        filename_stream_patterns = {
            r"^ENEDIS_R171_.+\.zip$": sgeproxy.streams.R171,
            r"^ERDF_R151_.+\.zip$": sgeproxy.streams.R151,
            r"^ERDF_R50_.+\.zip$": sgeproxy.streams.R50,
            r"^ENEDIS_.+_R4Q_CDC_.+\.zip$": sgeproxy.streams.R4x,
        }

        stream_handler = None
        for pattern in filename_stream_patterns:
            if re.match(pattern, filename):
                stream_handler = filename_stream_patterns[pattern]
                break

        if stream_handler is None:
            logging.error(f"No handler for file {path}")
            return

        with self.open(path) as data_files:
            for data_file in data_files:
                stream = stream_handler(data_file)
                for metadata, record in stream.records():
                    yield metadata, record  # or yield stream.records()?

        self.archive(path)


class StreamFiles:
    def __init__(self, inbox_file_path, aes_iv, aes_key):

        self.inbox_file_path = inbox_file_path
        self.aes_iv = aes_iv
        self.aes_key = aes_key

    def __enter__(self):

        self.temp_dir = tempfile.mkdtemp()

        filename = os.path.basename(self.inbox_file_path)
        temp_file = os.path.join(self.temp_dir, filename)

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

        # If unencrypted file is a zip archive, extract it
        if temp_file.endswith(".zip"):
            subprocess.run(["unzip", temp_file, "-d", self.temp_dir], check=True)
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
        self.records: Dict[str, Tuple[SgeProxyMeta, List[Record]]] = {}

    def add(self, metadata: SgeProxyMeta, record: Record) -> None:
        if record.name not in self.records:
            self.records[record.name] = (metadata, [record])
        else:
            # All records sharing the same name should share the same metadata
            assert metadata == self.records[record.name][0]
            self.records[record.name][1].append(record)

    def get(
        self, prefix: str = "", chunk_size: int = 0
    ) -> Iterable[Tuple[SgeProxyMeta, List[Record]]]:
        for name, meta_with_records in self.records.items():
            meta, records = meta_with_records
            if name.startswith(prefix):
                if chunk_size:
                    for records_chunk in chunks(records, chunk_size):
                        yield meta, records_chunk
                else:
                    yield meta, records


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
        conf["streams"]["aes_iv"],
        conf["streams"]["aes_key"],
        publish_archives=args.publish_archives,
    )

    records_by_name = RecordsByName()

    for f in streams_files.glob():
        logging.info(f"Parsing {f}")
        try:
            for f in streams_files.glob():
                for metadata, record in streams_files.file_records(f):
                    records_by_name.add(metadata, record)
        except Exception:
            logging.exception(f"Unable to parse data from {f}")
            streams_files.move_to_errors(f)

    xmpp.connect()

    loop = asyncio.get_event_loop()

    loop.run_until_complete(asyncio.wait_for(xmpp.session_started, 10))

    for sub in db_session.query(Subscription).all():
        # TODO move check to query
        if args.user and args.user != sub.user_id:
            continue

        series_name = f"urn:dev:prm:{sub.usage_point_id}_{sub.series_name}"

        for metadata, records in records_by_name.get(
            prefix=series_name, chunk_size=1000
        ):
            with sub.notification_checks():
                loop.run_until_complete(xmpp.send_data(sub.user_id, metadata, records))

    xmpp.disconnect()
