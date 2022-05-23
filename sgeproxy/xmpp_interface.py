#!/usr/bin/env python3

import logging
import datetime as dt

from slixmpp.exceptions import XMPPError
from sqlalchemy.exc import IntegrityError

import re

from sgeproxy.sge import SgeError
from sgeproxy.db import User, WebservicesCall, WebservicesCallStatus, now_local
import quoalise


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


class GetHistory:

    SAMPLE_IDENTIFIER = "urn:dev:prm:00000000000000_consumption/power/active/raw"

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

        end_date = dt.datetime.today()
        start_date = end_date - dt.timedelta(days=1)

        form.addField(
            var="start_date",
            ftype="text-single",
            label="Start date",
            desc=" Au format YYYY-MM-DD",
            required=True,
            value=start_date.strftime("%Y-%m-%d"),
        )

        form.addField(
            var="end_date",
            ftype="text-single",
            label="End date",
            desc=" Au format YYYY-MM-DD",
            required=True,
            value=end_date.strftime("%Y-%m-%d"),
        )

        session["payload"] = form
        session["next"] = self.handle_submit

        return session

    def handle_submit(self, payload, session):

        identifier = payload["values"]["identifier"]
        start_date = quoalise.parse_iso_date(payload["values"]["start_date"])
        end_date = quoalise.parse_iso_date(payload["values"]["end_date"])

        logging.info(f"{session['from']} {identifier} {start_date} {end_date}")

        m = re.match(r"^urn:dev:prm:(\d{14})_(.*)$", identifier)
        if not m:
            raise XMPPError(
                condition="bad-request",
                etype="modify",
                text="Unexpected record identifer "
                + f"('{identifier}', should be like '{self.SAMPLE_IDENTIFIER}')",
            )

        usage_point_id = m.group(1)
        measurement = m.group(2)

        with self.db_session_maker() as db:

            user = db.query(User).get(session["from"].bare)
            if user is None:
                raise XMPPError(
                    condition="not-authorized",
                    text=f'Unknown user {session["from"].bare}',
                )

            try:
                call_date = now_local()
                consent = user.consent_for(db, usage_point_id, call_date)

                call = WebservicesCall(
                    usage_point_id=usage_point_id,
                    user=user,
                    consent=consent,
                    called_at=call_date,
                )
                db.add(call)
                db.commit()

            except (PermissionError, IntegrityError) as e:
                raise XMPPError(condition="not-authorized", text=str(e))

            try:
                data = self.data_provider(
                    measurement, usage_point_id, start_date, end_date
                )
                call.status = WebservicesCallStatus.OK
            except SgeError as e:
                call.status = WebservicesCallStatus.FAILED
                call.error = e.code
                return fail_with(e.message, e.code)
            except ValueError as e:
                raise XMPPError(
                    condition="bad-request",
                    etype="modify",
                    text=str(e),
                )

            finally:
                db.commit()

        form = self.xmpp_client["xep_0004"].make_form(
            ftype="result", title="Get history"
        )

        form.addField(
            var="result", ftype="fixed", label=f"Get {identifier}", value="Success"
        )

        session["next"] = None
        session["payload"] = [form, data]

        return session
