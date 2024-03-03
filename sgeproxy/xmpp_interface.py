#!/usr/bin/env python3

import logging
import datetime as dt
import pytz

from slixmpp.exceptions import XMPPError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import re

import sgeproxy.sge
from sgeproxy.sge import SgeError, DetailedMeasurementsV3, TechnicalData
from sgeproxy.db import (
    User,
    UsagePoint,
    UsagePointSegment,
    Consent,
    ConsentIssuerType,
    WebservicesCall,
    CheckedWebserviceCall,
    Subscription,
    WebservicesCallsSubscriptions,
    SubscriptionType,
    now_local,
)


# TODO convert to QuoaliseException, extending XMPPError
def fail_with_sge(message, code):

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
    if m:
        usage_point_id = m.group(1)
        series_name = m.group(2)
        return usage_point_id, series_name
    m = re.match(r"^urn:dev:prm:(\d{14})$", identifier)
    if m:
        usage_point_id = m.group(1)
        return usage_point_id, None
    else:
        raise XMPPError(
            condition="bad-request",
            etype="modify",
            text="Unexpected record identifer "
            + f"('{identifier}', should be like '{SAMPLE_IDENTIFIER}')",
        )


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
                    webservice=DetailedMeasurementsV3.SERVICE_NAME,
                    usage_point_id=usage_point_id,
                    user=user,
                    consent=consent,
                )
                with CheckedWebserviceCall(call, db):
                    logging.info(
                        f"{session['from']} {identifier} history "
                        f"{start_time} {end_time}"
                    )
                    data = self.data_provider(
                        measurement, usage_point_id, start_time, end_time
                    )

            except (PermissionError, IntegrityError) as e:
                raise XMPPError(condition="not-authorized", text=str(e))
            except SgeError as e:
                return fail_with_sge(e.message, e.code)
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


class Subscribe:
    def __init__(
        self,
        xmpp_client,
        db_session_maker,
        technichal_data_provider,
        subscription_provider,
    ):
        self.xmpp_client = xmpp_client
        self.db_session_maker = db_session_maker
        self.technichal_data_provider = technichal_data_provider
        self.subscription_provider = subscription_provider

    def handle_request(self, iq, session):

        if iq["command"].xml:  # has subelements
            return self.handle_submit(session["payload"], session)

        form = self.xmpp_client["xep_0004"].make_form(ftype="form", title="Subscribe")

        form.addField(
            var="identifier",
            ftype="text-single",
            label="Identifier",
            required=True,
            value=SAMPLE_IDENTIFIER,
        )

        session["payload"] = form
        session["next"] = self.handle_submit

        return session

    def get_or_call_sge_subscription(
        self,
        db_session: Session,
        user: User,
        usage_point: UsagePoint,
        consent: Consent,
        call_type: SubscriptionType,
    ):

        existing_call = WebservicesCallsSubscriptions.find_existing(
            db_session, usage_point.id, call_type
        )

        if existing_call is not None:
            return existing_call

        call = WebservicesCall(
            usage_point=usage_point,
            consent=consent,
            user=user,
            webservice=sgeproxy.sge.Subscribe.SERVICE_NAME,
        )

        expires_at = min(consent.expires_at, now_local() + dt.timedelta(days=365))

        with CheckedWebserviceCall(call, db_session):
            logging.info(f"{user.bare_jid} {usage_point.id} subscribe")
            resp = self.subscription_provider(
                usage_point_id=usage_point.id,
                call_type=call_type,
                expires_at=expires_at,
                is_linky=usage_point.segment
                in [UsagePointSegment.C5, UsagePointSegment.P4],
                consent_issuer_is_company=consent.issuer_type
                == ConsentIssuerType.COMPANY,
                consent_issuer_name=consent.issuer_name,
            )

            print(resp)

            return WebservicesCallsSubscriptions(
                webservices_call=call,
                call_type=call_type,
                expires_at=expires_at,
                call_id=int(resp.serviceSouscritId),
            )

    def handle_submit(self, payload, session):

        identifier = payload["values"]["identifier"]

        usage_point_id, measurement = parse_identifier(identifier)

        if measurement not in [
            "consumption/energy/active/index",
            "consumption/power/apparent/max",
            "consumption/power/active/raw",
        ]:
            raise XMPPError(
                condition="bad-request",
                etype="modify",
                text=f"Subscription to {measurement} is not supported",
            )

        with self.db_session_maker() as db:

            user = db.query(User).get(session["from"].bare)
            if user is None:
                raise XMPPError(
                    condition="not-authorized",
                    text=f'Unknown user {session["from"].bare}',
                )

            subscription = (
                db.query(Subscription)
                .filter(Subscription.user_id == user.bare_jid)
                .filter(Subscription.usage_point_id == usage_point_id)
                .filter(Subscription.series_name == measurement)
            ).first()

            if subscription is not None:
                # Return ok for now when already subscribed
                # raise XMPPError(
                #     condition="bad-request",
                #     etype="modify",
                #     text=f"Already subscribed to {identifier}",
                # )
                pass
            else:
                try:
                    consent = user.consent_for(db, usage_point_id)
                    usage_point = db.query(UsagePoint).get(usage_point_id)

                    if usage_point.segment is None or usage_point.service_level is None:

                        call = WebservicesCall(
                            webservice=TechnicalData.SERVICE_NAME,
                            usage_point_id=usage_point_id,
                            user=user,
                            consent=consent,
                        )
                        with CheckedWebserviceCall(call, db):
                            logging.info(
                                f"{session['from']} {identifier} technical data"
                            )
                            technical_data = self.technichal_data_provider(
                                usage_point_id
                            )

                        usage_point.segment = UsagePointSegment[
                            technical_data.donneesGenerales.segment.libelle
                        ]
                        if hasattr(
                            technical_data.donneesGenerales, "niveauOuvertureServices"
                        ):
                            usage_point.service_level = int(
                                technical_data.donneesGenerales.niveauOuvertureServices
                            )

                    subscription = Subscription(
                        user=user,
                        usage_point=usage_point,
                        series_name=measurement,
                        consent=consent,
                    )

                    db.commit()

                    if measurement == "consumption/power/active/raw":

                        call = self.get_or_call_sge_subscription(
                            db_session=db,
                            user=user,
                            usage_point=usage_point,
                            consent=consent,
                            call_type=SubscriptionType.CONSUMPTION_CDC_ENABLE,
                        )
                        subscription.webservices_calls.append(call)
                        db.commit()

                        call = self.get_or_call_sge_subscription(
                            db_session=db,
                            user=user,
                            usage_point=usage_point,
                            consent=consent,
                            call_type=SubscriptionType.CONSUMPTION_CDC_RAW,
                        )
                        subscription.webservices_calls.append(call)
                        db.commit()

                    elif measurement in [
                        "consumption/energy/active/index",
                        "consumption/power/apparent/max",
                    ]:
                        call = self.get_or_call_sge_subscription(
                            db_session=db,
                            user=user,
                            usage_point=usage_point,
                            consent=consent,
                            call_type=SubscriptionType.CONSUMPTION_IDX,
                        )
                        subscription.webservices_calls.append(call)
                        db.commit()

                    else:
                        raise RuntimeError(f"Unexpected measurement {measurement}")

                except (PermissionError, IntegrityError) as e:
                    raise XMPPError(condition="not-authorized", text=str(e))
                except SgeError as e:
                    # SGT570: Le service demandé est déjà actif sur la période demandée.
                    # Service is already active
                    # TODO fetch the id and add it to our known subscription calls
                    if e.code == "SGT570":
                        pass
                    else:
                        return fail_with_sge(e.message, e.code)
                except ValueError as e:
                    raise XMPPError(
                        condition="bad-request",
                        etype="modify",
                        text=str(e),
                    )

        form = self.xmpp_client["xep_0004"].make_form(ftype="result", title="Subscribe")

        form.addField(
            var="result",
            ftype="fixed",
            label=f"Subscribe to {identifier}",
            value="Success",
        )

        session["next"] = None
        # session["payload"] = [form, data]
        session["payload"] = [form]

        return session


class Unsubscribe:
    def __init__(self, xmpp_client, db_session_maker, unsubscription_provider):
        self.xmpp_client = xmpp_client
        self.db_session_maker = db_session_maker
        self.unsubscription_provider = unsubscription_provider

    def handle_request(self, iq, session):

        if iq["command"].xml:  # has subelements
            return self.handle_submit(session["payload"], session)

        form = self.xmpp_client["xep_0004"].make_form(ftype="form", title="Unsubscribe")

        form.addField(
            var="identifier",
            ftype="text-single",
            label="Identifier",
            required=True,
            value=SAMPLE_IDENTIFIER,
        )

        session["payload"] = form
        session["next"] = self.handle_submit

        return session

    def handle_submit(self, payload, session):

        identifier = payload["values"]["identifier"]

        usage_point_id, measurement = parse_identifier(identifier)

        with self.db_session_maker() as db:

            user = db.query(User).get(session["from"].bare)
            if user is None:
                raise XMPPError(
                    condition="not-authorized",
                    text=f'Unknown user {session["from"].bare}',
                )

            subscriptions = (
                db.query(Subscription)
                .filter(Subscription.user_id == user.bare_jid)
                .filter(Subscription.usage_point_id == usage_point_id)
            )

            try:
                for subscription in subscriptions.all():
                    # Remove all subscriptions matching name
                    if measurement is None or subscription.series_name.startswith(
                        measurement
                    ):
                        subscription.webservices_calls[:] = []
                        db.delete(subscription)
                db.commit()

                # Cleanup
                # TODO, do this later asynchronously?
                # for sub_call in WebservicesCallsSubscriptions.query_unused(db):

                #     call = WebservicesCall(
                #         usage_point_id=usage_point_id,
                #         consent=sub_call.consent,
                #         user=sub_call.user,
                #         webservice=sgeproxy.sge.Unsubscribe.SERVICE_NAME,
                #     )

                #     with CheckedWebserviceCall(call, self.session):
                #         logging.info(f"{user.bare_jid} {usage_point_id} unsubscribe")

                #         self.unsubscription_provider(
                #             usage_point_id=usage_point_id,
                #             call_id=sub_call.call_id,
                #         )
                #         db.delete(sub_call)
                #         db.commit()

            except (PermissionError, IntegrityError) as e:
                raise XMPPError(condition="not-authorized", text=str(e))
            except SgeError as e:
                return fail_with_sge(e.message, e.code)
            except ValueError as e:
                raise XMPPError(
                    condition="bad-request",
                    etype="modify",
                    text=str(e),
                )

        form = self.xmpp_client["xep_0004"].make_form(
            ftype="result", title="Unsubscribe"
        )

        form.addField(
            var="result",
            ftype="fixed",
            label=f"Unsubscribe to {identifier}",
            value="Success",
        )

        session["next"] = None
        # session["payload"] = [form, data]
        session["payload"] = [form]

        return session
