#!/usr/bin/env python3

import slixmpp
import asyncio
import logging
import os

import sge
import xmpp_interface
from authorization import Authorizations

async def main(conf, sge_credentials):

    logging.info(f"Using SGE user {sge_credentials['login']} on {sge_credentials['environment']} environment")

    authorizations = Authorizations.from_conf(conf)

    detailed_measurements = sge.DetailedMeasurements(sge_credentials)

    xmpp_client = slixmpp.ClientXMPP(conf['xmpp']['full_jid'], conf['xmpp']['password'])

    xmpp_client.register_plugin('xep_0004')
    xmpp_client.register_plugin('xep_0050')
    xmpp_client.register_plugin('xep_0199', {'keepalive': True, 'frequency':15})

    xmpp_client.connect()

    await xmpp_client.wait_until('session_start')

    xmpp_client.send_presence()

    handler = xmpp_interface.GetMeasurements(xmpp_client,
                                             authorizations.validate,
                                             detailed_measurements.get_measurements)
    xmpp_client['xep_0050'].add_command(node='get_records',
                                        name='Get records',
                                        handler=handler.handle_request)

if __name__ == '__main__':

    import argparse
    import sys
    import config
    import datetime as dt
    import logging.handlers

    logging.basicConfig()
    debug = []

    debug.append("slixmpp")

    for logger in debug:
        logger = logging.getLogger(logger)
        logger.setLevel(logging.DEBUG)
        logger.propagate = True

    parser = argparse.ArgumentParser()
    parser.add_argument('conf', help="Configuration file (typically private/*.conf.json)")
    parser.add_argument('sge_credentials', help="Credentials to access SGE Tiers (typically private/*.conf.json)")
    parser.add_argument('--log-level', default="INFO")
    args = parser.parse_args()

    conf = config.File(args.conf)
    sge_credentials = config.File(args.sge_credentials)

    logger = logging.getLogger()
    # Log info to a file, ignoring the log level from command line
    # (always include data access details, rotates every monday)
    os.makedirs(conf['logs_dir'], exist_ok=True)
    file = logging.handlers.TimedRotatingFileHandler(
        os.path.join(conf['logs_dir'], "proxy.log"), when='W0', interval=1, backupCount=0, atTime=dt.time.min)
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