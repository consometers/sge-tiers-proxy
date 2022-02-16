#!/usr/bin/env python3

import logging
import asyncio
import datetime as dt

from slixmpp import ClientXMPP
from slixmpp.exceptions import XMPPError
from slixmpp.xmlstream import ElementBase, ET, register_stanza_plugin

import json
import re

from sge import SgeError
from authorization import MeasurementScope, HistoryScope
import quoalise

# TODO convert to QuoaliseException, extending XMPPError
def fail_with(message, code):

    args = {'issuer': 'enedis-sge-tiers'} # TODO directly use a xml ns?
    if code is not None:
        args['code'] = code

    logging.error(f"code: {repr(code)}, message: {message}")
    raise XMPPError(extension="upstream-error",
                    extension_ns="urn:quoalise:0",
                    extension_args=args,
                    text=message,
                    etype="cancel")

class GetMeasurements:

    SAMPLE_IDENTIFIER = "urn:dev:prm:00000000000000_consumption/active_power/raw"

    def __init__(self, xmpp_client, request_validator, data_provider):
        self.xmpp_client = xmpp_client
        self.request_validator = request_validator
        self.data_provider = data_provider

    def handle_request(self, iq, session):

        if iq['command'].xml: # has subelements
            return self.handle_submit(session['payload'], session)

        form = self.xmpp_client['xep_0004'].make_form(ftype='form', title='Get measurements')

        form.addField(var='identifier',
                      ftype='text-single',
                      label='Identifier',
                      required=True,
                      value=self.SAMPLE_IDENTIFIER)

        end_date = dt.datetime.today()
        start_date = end_date - dt.timedelta(days=1)

        form.addField(var='start_date',
                      ftype='text-single',
                      label='Start date',
                      desc=' Au format YYYY-MM-DD',
                      required=True,
                      value=start_date.strftime('%Y-%m-%d'))

        form.addField(var='end_date',
                      ftype='text-single',
                      label='End date',
                      desc=' Au format YYYY-MM-DD',
                      required=True,
                      value=end_date.strftime('%Y-%m-%d'))

        session['payload'] = form
        session['next'] = self.handle_submit

        return session

    def handle_submit(self, payload, session):

        identifier = payload['values']['identifier']
        start_date = quoalise.parse_iso_date(payload['values']['start_date'])
        end_date = quoalise.parse_iso_date(payload['values']['end_date'])

        logging.info(f"{session['from']} {identifier} {start_date} {end_date}")

        m = re.match(r'^urn:dev:prm:(\d{14})_(.*)$', identifier)
        if not m:
            return fail_with(f"Unexpected measurement identifer ('{identifier}', should be like '{self.SAMPLE_IDENTIFIER}')")

        usage_point_id = m.group(1)
        measurement = m.group(2)

        try:
            measurement_scope = MeasurementScope(
                details=True,
                consumption='consumption' in identifier,
                production='production' in identifier,
                history=HistoryScope(date_from=start_date, date_to=end_date))
            self.request_validator(session['from'].bare, usage_point_id,
                                   measurement_scope=measurement_scope)
        except PermissionError as e:
            raise XMPPError(condition='not-authorized', text=str(e))

        try:
            data = self.data_provider(measurement, usage_point_id, start_date, end_date)
        except SgeError as e:
            # TODO does session needs cleanup?
            return fail_with(e.message, e.code)

        form = self.xmpp_client['xep_0004'].make_form(ftype='result', title=f"Get measurements")

        form.addField(var='result',
                      ftype='fixed',
                      label=f'Get {identifier}',
                      value=f"Success")

        session['next'] = None
        session['payload'] = [form, data]

        return session