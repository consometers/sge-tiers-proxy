#!/usr/bin/env python3

import asyncio

import unittest
import quoalise
import datetime as dt
import pytz
from typing import Tuple
from statistics import mean

CONF_CLIENT = None
CONF_PROXY = None


def parse_iso_date(date_str):
    return dt.datetime.strptime(date_str, "%Y-%m-%d").date()


def previous_days(days: int) -> Tuple[dt.datetime, dt.datetime]:
    paris_tz = pytz.timezone("Europe/Paris")
    end_time = paris_tz.localize(dt.datetime.now()).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_time = paris_tz.normalize(end_time - dt.timedelta(days=days))

    return start_time, end_time


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

        self.proxy = CONF_PROXY["xmpp"]["full_jid"]

    def tearDown(self):
        self.client.disconnect()

    # TODO xmpp user not in database
    # def test_unauthorized_xmpp_user(self):
    #     pass

    def test_get_authorized_consumption_power_active(self):
        measurement = (
            f"urn:dev:prm:{self.usage_point['id']}_consumption/power/active/raw"
        )
        start_time, end_time = previous_days(7)
        data = self.client.get_history(self.proxy, measurement, start_time, end_time)
        records = list(data.records)
        self.assertGreaterEqual(len(records), 0)
        self.assertEqual(records[0].name, measurement)

    def test_get_authorized_consumption_energy_daily(self):
        measurement = (
            f"urn:dev:prm:{self.usage_point['id']}_consumption/energy/active/daily"
        )
        start_time, end_time = previous_days(30)
        data = self.client.get_history(self.proxy, measurement, start_time, end_time)
        records = list(data.records)
        self.assertGreaterEqual(len(records), 0)
        self.assertEqual(records[0].name, measurement)

    def test_get_authorized_production_power_active(self):
        measurement = (
            f"urn:dev:prm:{self.usage_point['id']}_production/power/active/raw"
        )
        start_time, end_time = previous_days(7)
        data = self.client.get_history(self.proxy, measurement, start_time, end_time)
        records = list(data.records)
        self.assertGreaterEqual(len(records), 0)
        self.assertEqual(records[0].name, measurement)

    def test_get_authorized_production_energy_daily(self):
        measurement = (
            f"urn:dev:prm:{self.usage_point['id']}_production/energy/active/daily"
        )
        start_time, end_time = previous_days(30)
        data = self.client.get_history(self.proxy, measurement, start_time, end_time)
        records = list(data.records)
        self.assertGreaterEqual(len(records), 0)
        self.assertEqual(records[0].name, measurement)

    def test_get_unauthorized(self):
        measurement = (
            f"urn:dev:prm:{self.usage_point['id']}_consumption/power/active/raw"
        )
        start_time, end_time = previous_days(7)
        with self.assertRaises(quoalise.NotAuthorized) as context:
            self.client.get_history(self.proxy, measurement, start_time, end_time)

        self.assertTrue(self.usage_point["id"] in str(context.exception))

    def test_get_unexisting_resource(self):
        measurement = f"urn:dev:prm:{self.usage_point['id']}_does/not/exists"
        start_time, end_time = previous_days(7)
        with self.assertRaises(quoalise.BadRequest):
            self.client.get_history(self.proxy, measurement, start_time, end_time)

        measurement = "fdoes/not/exists"
        with self.assertRaises(quoalise.BadRequest):
            self.client.get_history(self.proxy, measurement, start_time, end_time)

    def test_load_curve_matches_energy(self):
        start_time, end_time = previous_days(7)
        load_curve_data = self.client.get_history(
            self.proxy,
            f"urn:dev:prm:{self.usage_point['id']}_consumption/power/active/raw",
            start_time,
            end_time,
        )
        energy_data = self.client.get_history(
            self.proxy,
            f"urn:dev:prm:{self.usage_point['id']}_consumption/energy/active/daily",
            start_time,
            end_time,
        )

        load_curve_records = list(load_curve_data.records)
        energy_records = list(energy_data.records)

        self.assertGreaterEqual(len(load_curve_records), 1)
        self.assertGreaterEqual(len(energy_records), 1)

        total_energy_wh = sum([record.value for record in energy_records])
        mean_power_w = mean([record.value for record in load_curve_records])
        computed_energy_wh = len(energy_records) * 24 * mean_power_w

        self.assertEqual(computed_energy_wh, total_energy_wh)

    def test_subscribe(self):

        measurement = (
            f"urn:dev:prm:{self.usage_point['id']}_consumption/power/active/raw"
        )
        self.client.unsubscribe(self.proxy, measurement)
        self.client.subscribe(self.proxy, measurement)

    def test_subscribe_unsupported(self):

        measurement = f"urn:dev:prm:{self.usage_point['id']}_random_stuff"
        with self.assertRaises(quoalise.BadRequest):
            self.client.subscribe(self.proxy, measurement)


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
