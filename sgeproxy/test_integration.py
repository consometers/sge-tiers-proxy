#!/usr/bin/env python3

import asyncio

import unittest
import quoalise
import datetime as dt

CONF_CLIENT = None
CONF_PROXY = None


def parse_iso_date(date_str):
    return dt.datetime.strptime(date_str, "%Y-%m-%d").date()


class TestGetMeasurement(unittest.TestCase):
    def setUp(self):
        test_name = self.id().split(".test_")[-1]
        usage_point_name = CONF_CLIENT["tests"].get(test_name)
        if usage_point_name:
            self.usage_point = CONF_CLIENT["usage_points"][usage_point_name]

        self.loop = asyncio.get_event_loop()

        if "address" in CONF_CLIENT["xmpp"] and "port" in CONF_CLIENT["xmpp"]:
            address = (CONF_CLIENT["xmpp"]["address"], CONF_CLIENT["xmpp"]["port"])
        else:
            address = None

        self.client = quoalise.Client.connect(
            CONF_CLIENT["xmpp"]["bare_jid"],
            CONF_CLIENT["xmpp"]["password"],
            CONF_PROXY["xmpp"]["full_jid"],
            address=address,
        )

    def tearDown(self):
        self.client.disconnect()

    # def test_unauthorized_xmpp_user(self):
    #     pass

    # test_accesses_are_logged

    def test_get_authorized_consumption_active_power(self):
        measurement = (
            f"urn:dev:prm:{self.usage_point['id']}_consumption/active_power/raw"
        )
        end_date = dt.date.today() - dt.timedelta(days=1)
        start_date = end_date - dt.timedelta(days=6)
        measurements = self.client.get_records(measurement, start_date, end_date)
        print(measurements)

    def test_get_authorized_consumption_daily_energy(self):
        measurement = f"urn:dev:prm:{self.usage_point['id']}_consumption/energy/daily"
        end_date = dt.date.today() - dt.timedelta(days=1)
        start_date = end_date - dt.timedelta(days=30)
        measurements = self.client.get_records(measurement, start_date, end_date)
        print(measurements)

    def test_get_authorized_production_active_power(self):
        measurement = (
            f"urn:dev:prm:{self.usage_point['id']}_production/active_power/raw"
        )
        end_date = dt.date.today() - dt.timedelta(days=1)
        start_date = end_date - dt.timedelta(days=6)
        measurements = self.client.get_records(measurement, start_date, end_date)
        print(measurements)

    def test_get_authorized_production_daily_energy(self):
        measurement = f"urn:dev:prm:{self.usage_point['id']}_production/energy/daily"
        end_date = dt.date.today() - dt.timedelta(days=1)
        start_date = end_date - dt.timedelta(days=30)
        measurements = self.client.get_records(measurement, start_date, end_date)
        print(measurements)

    def test_get_unauthorized(self):
        measurement = (
            f"urn:dev:prm:{self.usage_point['id']}_consumption/active_power/raw"
        )
        end_date = dt.date.today() - dt.timedelta(days=1)
        start_date = end_date - dt.timedelta(days=6)
        with self.assertRaises(quoalise.NotAuthorized) as context:
            self.client.get_records(measurement, start_date, end_date)

        self.assertTrue(self.usage_point["id"] in str(context.exception))


if __name__ == "__main__":

    import sys
    import argparse
    import logging
    import sgeproxy.config

    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("conf_client", help="Client configuration file")
    parser.add_argument("conf_proxy", help="Proxy configuration file")
    args, unittest_args = parser.parse_known_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    CONF_CLIENT = sgeproxy.config.File(args.conf_client)
    CONF_PROXY = sgeproxy.config.File(args.conf_proxy)

    unittest.main(argv=[sys.argv[0]] + unittest_args)
