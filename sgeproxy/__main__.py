#!/usr/bin/env python3

import asyncio
import logging
import os
from typing import List

import slixmpp
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import sgeproxy.sge
import sgeproxy.xmpp_interface


async def main(conf, sge_credentials):

    logging.info(
        (
            f"Using SGE user {sge_credentials['login']} "
            f" on {sge_credentials['environment']} environment"
        )
    )

    db_engine = create_engine(conf["db"]["url"])
    db_session_maker = sessionmaker(bind=db_engine)

    detailed_measurements = sgeproxy.sge.DetailedMeasurements(sge_credentials)
    technical_data = sgeproxy.sge.TechnicalData(sge_credentials)
    subscribe = sgeproxy.sge.Subscribe(sge_credentials)
    unsubscribe = sgeproxy.sge.Unsubscribe(sge_credentials)

    xmpp_client = slixmpp.ClientXMPP(conf["xmpp"]["full_jid"], conf["xmpp"]["password"])

    xmpp_client.register_plugin("xep_0004")
    xmpp_client.register_plugin("xep_0050")
    xmpp_client.register_plugin("xep_0199", {"keepalive": True, "frequency": 15})

    if "address" in conf["xmpp"] and "port" in conf["xmpp"]:
        xmpp_client.connect(address=(conf["xmpp"]["address"], conf["xmpp"]["port"]))
    else:
        xmpp_client.connect()

    session_state = asyncio.Future()

    def session_start_waiter(event_data):
        session_state.set_result(True)

    xmpp_client.add_event_handler(
        "session_start",
        session_start_waiter,
        disposable=True,
    )

    await asyncio.wait_for(session_state, 10)

    xmpp_client.send_presence()

    history_handler = sgeproxy.xmpp_interface.GetHistory(
        xmpp_client, db_session_maker, detailed_measurements.get_measurements
    )
    xmpp_client["xep_0050"].add_command(
        node="get_history", name="Get history", handler=history_handler.handle_request
    )

    subscribe_handler = sgeproxy.xmpp_interface.Subscribe(
        xmpp_client,
        db_session_maker,
        technical_data.get_technical_data,
        subscribe.subscribe,
    )
    xmpp_client["xep_0050"].add_command(
        node="subscribe", name="Subscribe", handler=subscribe_handler.handle_request
    )

    unsubscribe_handler = sgeproxy.xmpp_interface.Unsubscribe(
        xmpp_client, db_session_maker, unsubscribe.unsubscribe
    )
    xmpp_client["xep_0050"].add_command(
        node="unsubscribe",
        name="Unsubscribe",
        handler=unsubscribe_handler.handle_request,
    )


if __name__ == "__main__":

    import argparse
    import sgeproxy.config
    import datetime as dt
    import logging.handlers

    logging.basicConfig()
    debug: List[str] = []

    # debug.append("slixmpp")

    for logger_name in debug:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        logger.propagate = True

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "conf", help="Configuration file (typically private/*.conf.json)"
    )
    parser.add_argument(
        "sge_credentials",
        help="Credentials to access SGE Tiers (typically private/*.conf.json)",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    conf = sgeproxy.config.File(args.conf)
    sge_credentials = sgeproxy.config.File(args.sge_credentials)

    logger = logging.getLogger()
    # Log info to a file, ignoring the log level from command line
    # (always include data access details, rotates every monday)
    os.makedirs(conf["logs_dir"], exist_ok=True)
    file = logging.handlers.TimedRotatingFileHandler(
        os.path.join(conf["logs_dir"], "proxy.log"),
        when="W0",
        interval=1,
        backupCount=0,
        atTime=dt.datetime.min,
    )
    file.setLevel(logging.INFO)
    file.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(file)
    # Log to console at the level asked by command line
    console = logging.StreamHandler()
    console.setLevel(getattr(logging, args.log_level.upper()))
    logger.addHandler(console)

    loop = asyncio.get_event_loop()

    loop.run_until_complete(main(conf, sge_credentials))
    loop.run_forever()
