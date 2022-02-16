#!/usr/bin/env python3

import slixmpp
import xml
from slixmpp.xmlstream import tostring
from xml.etree.ElementTree import fromstring
from xml.sax.saxutils import escape
import asyncio

import unittest
from client import SgeProxyClient
import quoalise
import datetime as dt

CONF_CLIENT = None
CONF_PROXY = None

def parse_iso_date(date_str):
    return dt.datetime.strptime(date_str, '%Y-%m-%d').date()

class TestGetMeasurement(unittest.TestCase):

    def setUp(self):
        test_name = self.id().split('.test_')[-1]
        usage_point_name = CONF_CLIENT['tests'].get(test_name)
        if usage_point_name:
            self.usage_point = CONF_CLIENT['usage_points'][usage_point_name]

        self.loop = asyncio.get_event_loop()
        self.client = self.loop.run_until_complete(
            SgeProxyClient.connect(
                CONF_CLIENT['xmpp']['bare_jid'],
                CONF_CLIENT['xmpp']['password'],
                CONF_PROXY['xmpp']['full_jid']))

    def tearDown(self):
        self.client.disconnect()

    # def test_unauthorized_xmpp_user(self):
    #     pass

    # test_accesses_are_logged

    def test_get_authorized_consumption_active_power(self):
        measurement = f"urn:dev:prm:{self.usage_point['id']}_consumption/active_power/raw"
        end_date = dt.date.today() - dt.timedelta(days=1)
        start_date = end_date - dt.timedelta(days=6)
        measurements = self.loop.run_until_complete(
            self.client.get_measurement(measurement, start_date, end_date))
        print(measurements)

    def test_get_authorized_production_active_power(self):
        measurement = f"urn:dev:prm:{self.usage_point['id']}_production/active_power/raw"
        end_date = dt.date.today() - dt.timedelta(days=1)
        start_date = end_date - dt.timedelta(days=6)
        measurements = self.loop.run_until_complete(
            self.client.get_measurement(measurement, start_date, end_date))
        print(measurements)

    def test_get_unauthorized(self):
        measurement = f"urn:dev:prm:{self.usage_point['id']}_consumption/active_power/raw"
        end_date = dt.date.today() - dt.timedelta(days=1)
        start_date = end_date - dt.timedelta(days=6)
        with self.assertRaises(quoalise.NotAuthorized) as context:
            measurements = self.loop.run_until_complete(
                self.client.get_measurement(measurement, start_date, end_date))

        self.assertTrue(self.usage_point['id'] in str(context.exception))

if __name__ == "__main__":

    import sys
    import argparse
    import logging
    import config

    parser = argparse.ArgumentParser()
    parser.add_argument('--log-level', default="INFO")
    parser.add_argument('conf_client', help='Client configuration file')
    parser.add_argument('conf_proxy', help='Proxy configuration file')
    args, unittest_args = parser.parse_known_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    CONF_CLIENT = config.File(args.conf_client)
    CONF_PROXY = config.File(args.conf_proxy)

    unittest.main(argv=[sys.argv[0]] + unittest_args)