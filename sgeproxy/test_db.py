import unittest

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.orm import sessionmaker
from sgeproxy.db import (
    Migration,
    SubscriptionStatus,
    User,
    UsagePoint,
    Consent,
    WebservicesCall,
    ConsentUsagePoint,
    Subscription,
    date_local,
)

import datetime as dt

# Database and access rights need to be initialized before running tests:
# sudo -u postgres psql
# CREATE DATABASE sgeproxy_test;
# CREATE USER sgeproxy_test WITH ENCRYPTED PASSWORD 'password';
# GRANT ALL PRIVILEGES ON DATABASE sgeproxy_test TO sgeproxy_test;
# \c sgeproxy_test
# ALTER SCHEMA public OWNER TO sgeproxy_test;
# \q


class Args:
    db_url = None


class TestDbMigrations(unittest.TestCase):
    def test_migrations_update_in_db_version_correctly(self):
        """
        Check that all migrations can be applied from an empty database,
        setting the current state (version) correctly.
        """

        engine = create_engine(Args.db_url)

        with engine.connect() as con:

            con.execute("DROP SCHEMA public CASCADE")
            con.execute("CREATE SCHEMA public")

            Migration.migrate(con)

            latest_version, _ = Migration.files()[-1]
            self.assertEqual(latest_version, Migration.deployed_version(con))

    def test_timestamps_are_not_naive(self):
        for _, file in Migration.files():
            with open(file, "r") as f:
                sql = f.read()
                sql = sql.upper()
                sql = " ".join(sql.split())
                start = 0
                expected_seq = " TIMESTAMP WITH TIME ZONE"
                while True:
                    start = sql.find(" TIMESTAMP", start)
                    if start < 0:
                        break
                    found_seq = sql[start : start + len(expected_seq)]
                    self.assertEqual(expected_seq, found_seq)
                    start += len(expected_seq)


class TestDbConsents(unittest.TestCase):
    def setUp(self):

        engine = create_engine(Args.db_url)

        with engine.connect() as con:

            con.execute("DROP SCHEMA public CASCADE")
            con.execute("CREATE SCHEMA public")

            Migration.migrate(con)

        Session = sessionmaker(bind=engine)
        self.session = Session()

        # Homer Simpson gave its consent to Alice,
        # allowing her to access his consumption data.

        self.alice = User(bare_jid="alice@wonderland.lit")
        self.session.add(self.alice)

        self.homer_usage_point = UsagePoint(id="09111642617347")
        self.session.add(self.homer_usage_point)

        self.homer_consent_to_alice = Consent(
            issuer_name="Simpson",
            issuer_type="male",
            begins_at=date_local(2020, 1, 1),
            expires_at=date_local(2021, 1, 1),
        )
        self.homer_consent_to_alice.usage_points.append(
            ConsentUsagePoint(usage_point=self.homer_usage_point, comment="Home")
        )
        self.session.add(self.homer_consent_to_alice)

        self.alice.consents.append(self.homer_consent_to_alice)

        # The Springfield Nuclear Power Plant gave its consent to Sister,
        # allowing her to access his production data

        self.sister = User(bare_jid="sister@realworld.lit")
        self.session.add(self.sister)

        self.burns_usage_point = UsagePoint(id="30001642617347")
        self.session.add(self.burns_usage_point)

        self.burns_consent_to_sister = Consent(
            issuer_name="The Springfield Nuclear Power Plant",
            issuer_type="company",
            begins_at=date_local(2020, 1, 1),
            expires_at=date_local(2021, 3, 1),
        )
        self.burns_consent_to_sister.usage_points.append(
            ConsentUsagePoint(usage_point=self.burns_usage_point, comment="Reactor #1")
        )
        self.session.add(self.burns_consent_to_sister)

        self.sister.consents.append(self.burns_consent_to_sister)

        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_naive_datetimes_are_not_allowed(self):
        consent = Consent(
            issuer_name="Simpson",
            issuer_type="male",
            begins_at=dt.datetime.now(),
            expires_at=dt.datetime.now() + dt.timedelta(days=30),
        )
        self.session.add(consent)
        with self.assertRaises(StatementError):
            self.session.commit()

    def test_db_allows_call_within_consent_scope(self):

        WebservicesCall(
            usage_point=self.homer_usage_point,
            consent=self.homer_consent_to_alice,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=date_local(2020, 1, 2),
        )

        self.session.commit()

    def test_db_denies_call_with_no_consent(self):

        WebservicesCall(
            usage_point=self.homer_usage_point,
            consent=None,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=date_local(2020, 1, 2),
        )

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_db_denies_call_for_an_out_of_scope_usage_point(self):
        """
        Homer consent does not allow to access Burns usage point data.
        """
        WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=self.homer_consent_to_alice,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=date_local(2020, 1, 2),
        )

        with self.assertRaises(IntegrityError) as context:
            self.session.commit()

        self.assertTrue(
            'is not present in table "consents_usage_points"' in str(context.exception)
        )

    def test_db_denies_call_with_no_usage_point(self):

        WebservicesCall(
            usage_point=None,
            consent=self.homer_consent_to_alice,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=date_local(2020, 1, 2),
        )

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_db_denies_call_for_an_out_of_scope_user(self):
        """
        Alice cannot use consent from Burns (given to sister).
        """

        WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=self.burns_consent_to_sister,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=date_local(2020, 1, 2),
        )

        with self.assertRaises(IntegrityError) as context:
            self.session.commit()

        self.assertTrue(
            'is not present in table "consents_users"' in str(context.exception)
        )

    def test_db_denies_call_with_no_user(self):
        WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=self.burns_consent_to_sister,
            user=None,
            webservice="ConsultationMesures",
            called_at=date_local(2020, 1, 2),
        )

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_db_denies_call_for_a_too_early_date(self):
        """
        Alice cannot access data before the period of time Homer consented to.
        """

        WebservicesCall(
            usage_point=self.homer_usage_point,
            consent=self.homer_consent_to_alice,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=date_local(2019, 1, 2),
        )

        with self.assertRaises(IntegrityError) as context:
            self.session.commit()

        self.assertTrue("violates check constraint" in str(context.exception))

    def test_db_denies_call_for_a_too_late_date(self):
        """
        Alice cannot access data after the period of time Homer consented to.
        """

        WebservicesCall(
            usage_point=self.homer_usage_point,
            consent=self.homer_consent_to_alice,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=date_local(2022, 1, 2),
        )

        with self.assertRaises(IntegrityError) as context:
            self.session.commit()

        self.assertTrue("violates check constraint" in str(context.exception))

    def test_db_denies_call_with_no_date(self):

        WebservicesCall(
            usage_point=self.homer_usage_point,
            consent=self.homer_consent_to_alice,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=None,
        )

        with self.assertRaises(IntegrityError) as context:
            self.session.commit()

        self.assertTrue("violates check constraint" in str(context.exception))

    def test_db_denies_updating_consent_user_if_it_breaks_a_call_constraint(self):
        """
        Alice cannot remove a consent she used to make calls with.
        """

        call = WebservicesCall(
            usage_point=self.homer_usage_point,
            consent=self.homer_consent_to_alice,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=date_local(2020, 1, 2),
        )
        self.session.add(call)

        self.alice.consents.remove(self.homer_consent_to_alice)

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_db_denies_updating_consent_usage_point_if_it_breaks_a_call_constraint(
        self,
    ):
        """
        A consent cannot update its usage point if calls have been done with them.

        Proper handling of this kind of error would be to create another consent related
        to this usage point, update the related calls to that consent and finally remove
        the usage point from the erroneous initial consent.
        """

        call = WebservicesCall(
            usage_point=self.homer_usage_point,
            consent=self.homer_consent_to_alice,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=date_local(2020, 1, 2),
        )
        self.session.add(call)

        consent_usage_point = self.session.query(ConsentUsagePoint).get(
            (self.homer_consent_to_alice.id, self.homer_usage_point.id)
        )
        self.homer_consent_to_alice.usage_points.remove(consent_usage_point)

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_db_denies_updating_consent_dates_if_it_breaks_a_call_constraint(self):
        """
        Consent date period cannot be updated if calls have been made outside
        of that period.
        """

        call = WebservicesCall(
            usage_point=self.homer_usage_point,
            consent=self.homer_consent_to_alice,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=date_local(2020, 1, 2),
        )
        self.session.add(call)

        self.homer_consent_to_alice.begins_at = date_local(2020, 1, 3)

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_db_find_consents(self):

        consent = self.alice.consent_for(
            self.session, self.homer_usage_point, date_local(2020, 1, 3)
        )
        self.assertEqual(consent, self.homer_consent_to_alice)

        consent = self.sister.consent_for(
            self.session, self.burns_usage_point, date_local(2020, 1, 3)
        )
        self.assertEqual(consent, self.burns_consent_to_sister)

    def test_db_throws_exception_when_no_consent_given_for_usage_point(self):

        with self.assertRaises(PermissionError) as context:
            self.alice.consent_for(
                self.session, self.burns_usage_point, date_local(2020, 1, 3)
            )

        with self.assertRaises(PermissionError) as context:
            self.sister.consent_for(
                self.session, self.homer_usage_point, date_local(2020, 1, 3)
            )

        with self.assertRaises(PermissionError) as context:
            self.sister.consent_for(
                self.session, "00000000000000", date_local(2020, 1, 3)
            )

        self.assertTrue("No consent registered for" in str(context.exception))

    def test_db_throws_exception_when_no_consent_has_not_started_yet(self):

        with self.assertRaises(PermissionError) as context:
            self.alice.consent_for(
                self.session, self.homer_usage_point, date_local(2019, 12, 31)
            )

        with self.assertRaises(PermissionError) as context:
            self.sister.consent_for(
                self.session, self.burns_usage_point, date_local(2019, 12, 31)
            )

        self.assertTrue("is not valid yet" in str(context.exception))

    def test_db_throws_exception_when_consent_has_expired(self):

        with self.assertRaises(PermissionError) as context:
            self.alice.consent_for(
                self.session, self.homer_usage_point, date_local(2021, 1, 1)
            )

        with self.assertRaises(PermissionError) as context:
            self.sister.consent_for(
                self.session, self.burns_usage_point, date_local(2021, 3, 1)
            )

        self.assertTrue("is no longer valid" in str(context.exception))

    def test_db_can_subscribe_when_conscent_is_valid(self):

        subscription = Subscription(
            user=self.alice,
            usage_point=self.homer_usage_point,
            series_name="consumption/energy/index",
            subscribed_at=date_local(2020, 2, 1),
            consent=self.alice.consent_for(
                self.session, self.homer_usage_point, date_local(2020, 2, 1)
            ),
        )

        self.session.add(subscription)
        self.session.commit()

    def test_db_can_notify_when_conscent_is_valid(self):

        subscription = Subscription(
            user=self.alice,
            usage_point=self.homer_usage_point,
            series_name="consumption/energy/index",
            subscribed_at=date_local(2020, 2, 1),
            consent=self.alice.consent_for(
                self.session, self.homer_usage_point, date_local(2020, 2, 1)
            ),
        )

        self.session.add(subscription)
        self.session.commit()

        with subscription.notification_checks(notified_at=date_local(2020, 2, 1)):
            pass  # Notify the client sucessfully

        self.assertEqual(subscription.status, SubscriptionStatus.OK)
        self.assertEqual(subscription.notified_at, date_local(2020, 2, 1))

    def test_db_notification_failure(self):

        subscription = Subscription(
            user=self.alice,
            usage_point=self.homer_usage_point,
            series_name="consumption/energy/index",
            subscribed_at=date_local(2020, 2, 1),
            consent=self.alice.consent_for(
                self.session, self.homer_usage_point, date_local(2020, 2, 1)
            ),
        )

        self.session.add(subscription)
        self.session.commit()

        try:
            with subscription.notification_checks(notified_at=date_local(2020, 2, 1)):
                raise RuntimeError("Something happens")
        except RuntimeError:
            pass

        self.assertEqual(subscription.status, SubscriptionStatus.FAILED)
        self.assertEqual(subscription.notified_at, date_local(2020, 2, 1))

    def test_db_cannot_notify_subscriber_when_consent_is_no_longer_valid(self):

        subscription = Subscription(
            user=self.alice,
            usage_point=self.homer_usage_point,
            series_name="consumption/energy/index",
            subscribed_at=date_local(2020, 2, 1),
            consent=self.alice.consent_for(
                self.session, self.homer_usage_point, date_local(2020, 2, 1)
            ),
        )

        self.session.add(subscription)
        self.session.commit()

        # User is supposed to set notification date before sending it,
        # using the database constraints to check consent validity
        subscription.status = None
        subscription.notified_at = date_local(2022, 2, 1)

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_db_can_subscribe_cannot_use_an_urelated_consent(self):

        subscription = Subscription(
            user=self.alice,
            usage_point=self.homer_usage_point,
            series_name="consumption/energy/index",
            subscribed_at=date_local(2020, 2, 1),
            consent=self.sister.consent_for(
                self.session, self.burns_usage_point, date_local(2020, 2, 1)
            ),
        )

        self.session.add(subscription)

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_db_cannot_subscribe_multiple_time_to_same_series(self):

        subscription = Subscription(
            user=self.alice,
            usage_point=self.homer_usage_point,
            series_name="consumption/energy/index",
            subscribed_at=date_local(2020, 2, 1),
            consent=self.alice.consent_for(
                self.session, self.homer_usage_point, date_local(2020, 2, 1)
            ),
        )

        self.session.add(subscription)

        subscription = Subscription(
            user=self.alice,
            usage_point=self.homer_usage_point,
            series_name="consumption/energy/index",
            subscribed_at=date_local(2020, 2, 1),
            consent=self.alice.consent_for(
                self.session, self.homer_usage_point, date_local(2020, 2, 1)
            ),
        )

        with self.assertRaises(IntegrityError):
            self.session.commit()


if __name__ == "__main__":

    import sys
    import argparse
    import logging

    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "db_url",
        help="postgresql://sgeproxy_test:password@127.0.0.1:5432/sgeproxy_test",
    )
    args, unittest_args = parser.parse_known_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    Args.db_url = args.db_url

    unittest.main(argv=[sys.argv[0]] + unittest_args)
