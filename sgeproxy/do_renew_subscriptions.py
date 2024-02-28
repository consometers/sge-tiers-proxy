#!/usr/bin/env python3

import sys
import argparse
import datetime as dt
import time
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sgeproxy.db import (
    Subscription,
    now_local,
)
import sgeproxy.config
import sgeproxy.sge
import sgeproxy.xmpp_interface

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "db_url", help="Like postgresql://sgeproxy:…@127.0.0.1:5433/sgeproxy"
    )
    parser.add_argument(
        "sge_credentials",
        help="Credentials to access SGE Tiers (typically private/*.conf.json)",
    )
    args = parser.parse_args()

    sge_credentials = sgeproxy.config.File(args.sge_credentials)
    sge_subscribe = sgeproxy.sge.Subscribe(sge_credentials)
    sge_technical_data = sgeproxy.sge.TechnicalData(sge_credentials)

    db_engine = create_engine(args.db_url)
    db_session_maker = sessionmaker(bind=db_engine)

    # Dummy xmpp subscribe handler to use one of its helpers (to move elsewhere)
    xmpp_subscribe = sgeproxy.xmpp_interface.Subscribe(
        None,
        db_session_maker,
        sge_technical_data.get_technical_data,
        sge_subscribe.subscribe,
    )

    now = now_local()

    with db_session_maker() as db:

        # Find all subscriptions for which no valid WebservicesCallsSubscription exists
        # but a consent for the usage point is valid

        for sub in db.query(Subscription).all():
            expired_calls = sub.expired_calls(now)
            if not expired_calls:
                continue
            try:
                consent = sub.user.consent_for(db, sub.usage_point, now)
            except PermissionError:
                continue
            print("------")
            print(f"{dt.datetime.now()} {sub.usage_point_id} {sub.series_name}")
            print(f"Needs renew {expired_calls} to {consent.expires_at}")

            sub.consent = consent
            db.commit()

            for call_type in expired_calls:
                try:
                    call = xmpp_subscribe.get_or_call_sge_subscription(
                        db_session=db,
                        user=sub.user,
                        usage_point=sub.usage_point,
                        consent=consent,
                        call_type=call_type,
                    )
                    sub.webservices_calls.append(call)
                    db.commit()

                    print(f"{call_type} OK")

                except Exception as e:
                    logging.exception(e)

            sys.stdout.flush()
            sys.stderr.flush()
            time.sleep(1)
