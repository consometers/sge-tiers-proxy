#!/usr/bin/env python3

import logging
import datetime as dt
import pytz

from slixmpp.exceptions import XMPPError
from sqlalchemy.exc import IntegrityError

import re

from sgeproxy.sge import SgeError
from sgeproxy.db import (
    User,
    WebservicesCall,
    CheckedWebserviceCall,
)


# TODO convert to QuoaliseException, extending XMPPError
def fail_with(message, code):

    args = {"issuer": "enedis-sge-tiers"}  # TODO directly use a xml ns?
    if code is not None:
        args["code"] = code

    logging.error(f"code: {repr(code)}, message: {message}")
    raise XMPPError(
        extension="upstream-error",
        extension_ns="urn:quoalise:0",
        extension_args=args,
        text=message,
        etype="cancel",
    )


SAMPLE_IDENTIFIER = "urn:dev:prm:00000000000000_consumption/power/active/raw"


def parse_identifier(identifier):
    m = re.match(r"^urn:dev:prm:(\d{14})_(.*)$", identifier)
    if not m:
        raise XMPPError(
            condition="bad-request",
            etype="modify",
            text="Unexpected record identifer "
            + f"('{identifier}', should be like '{SAMPLE_IDENTIFIER}')",
        )
    usage_point_id = m.group(1)
    series_name = m.group(2)

    return usage_point_id, series_name


class GetHistory:
    def __init__(self, xmpp_client, db_session_maker, data_provider):
        self.xmpp_client = xmpp_client
        self.db_session_maker = db_session_maker
        self.data_provider = data_provider

    def handle_request(self, iq, session):

        if iq["command"].xml:  # has subelements
            return self.handle_submit(session["payload"], session)

        form = self.xmpp_client["xep_0004"].make_form(ftype="form", title="Get history")

        form.addField(
            var="identifier",
            ftype="text-single",
            label="Identifier",
            required=True,
            value=self.SAMPLE_IDENTIFIER,
        )

        end_time = dt.datetime.now(pytz.timezone("Europe/Paris")).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        start_time = end_time - dt.timedelta(days=1)

        form.addField(
            var="start_time",
            ftype="text-single",
            label="Start date",
            desc="Au format ISO 8601",
            required=True,
            value=start_time.isoformat(),
        )

        form.addField(
            var="end_time",
            ftype="text-single",
            label="End date",
            desc="Au format ISO 8601",
            required=True,
            value=end_time.isoformat(),
        )

        session["payload"] = form
        session["next"] = self.handle_submit

        return session

    def handle_submit(self, payload, session):

        identifier = payload["values"]["identifier"]
        start_time = dt.datetime.fromisoformat(payload["values"]["start_time"])
        end_time = dt.datetime.fromisoformat(payload["values"]["end_time"])

        logging.info(f"{session['from']} {identifier} {start_time} {end_time}")

        usage_point_id, measurement = parse_identifier(identifier)

        with self.db_session_maker() as db:

            user = db.query(User).get(session["from"].bare)
            if user is None:
                raise XMPPError(
                    condition="not-authorized",
                    text=f'Unknown user {session["from"].bare}',
                )

            try:
                consent = user.consent_for(db, usage_point_id)
                call = WebservicesCall(
                    usage_point_id=usage_point_id,
                    user=user,
                    consent=consent,
                )
                with CheckedWebserviceCall(call, db):
                    data = self.data_provider(
                        measurement, usage_point_id, start_time, end_time
                    )

            except (PermissionError, IntegrityError) as e:
                raise XMPPError(condition="not-authorized", text=str(e))
            except SgeError as e:
                return fail_with(e.message, e.code)
            except ValueError as e:
                raise XMPPError(
                    condition="bad-request",
                    etype="modify",
                    text=str(e),
                )

        form = self.xmpp_client["xep_0004"].make_form(
            ftype="result", title="Get history"
        )

        form.addField(
            var="result", ftype="fixed", label=f"Get {identifier}", value="Success"
        )

        session["next"] = None
        session["payload"] = [form, data]

        return session
