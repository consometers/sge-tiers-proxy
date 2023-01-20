import unittest

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.orm import sessionmaker
from sgeproxy.db import (
    Migration,
    SubscriptionStatus,
    User,
    UsagePoint,
    UsagePointSegment,
    Consent,
    WebservicesCall,
    CheckedWebserviceCall,
    WebservicesCallStatus,
    ConsentUsagePoint,
    Subscription,
    WebservicesCallsSubscriptions,
    SubscriptionType,
    date_local,
    now_local,
)
from sgeproxy.sge import SgeError

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


class TestDbIntegrity(unittest.TestCase):
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
            issuer_type="INDIVIDUAL",
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
            issuer_type="COMPANY",
            begins_at=date_local(2020, 1, 1),
            expires_at=now_local() + dt.timedelta(days=10),
        )
        self.burns_consent_to_sister.usage_points.append(
            ConsentUsagePoint(usage_point=self.burns_usage_point, comment="Reactor #1")
        )
        self.session.add(self.burns_consent_to_sister)

        self.sister.consents.append(self.burns_consent_to_sister)

        self.genius = User(bare_jid="genius@realworld.lit")
        self.session.add(self.genius)

        self.genius_open_consent = Consent(
            issuer_name="Genius",
            issuer_type="COMPANY",
            is_open=True,
            begins_at=date_local(2020, 1, 1),
            expires_at=date_local(2021, 1, 1),
        )
        self.genius.consents.append(self.genius_open_consent)

        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_naive_datetimes_are_not_allowed(self):
        consent = Consent(
            issuer_name="Simpson",
            issuer_type="INDIVIDUAL",
            begins_at=dt.datetime.now(),
            expires_at=dt.datetime.now() + dt.timedelta(days=30),
        )
        self.session.add(consent)
        with self.assertRaises(StatementError):
            self.session.commit()

    def test_enum_when_creating_objects(self):

        # Be careful, name is used when converted as SQL, not value

        point = UsagePoint(id="00000000000000", segment="C5")

        # When set from string, it cannot be compared to an enum right away
        self.assertNotEqual(point.segment, UsagePointSegment.C5)
        self.assertEqual(point.segment, "C5")

        self.session.add(point)
        self.session.commit()

        # But after commit or retreival from db,
        # it cannot be compared to a string…

        self.assertEqual(point.segment, UsagePointSegment.C5)
        self.assertNotEqual(point.segment, "C5")

        # Instead of assigning a string, a more consistent behavior can be
        # achieved by setting an enum from string value at creation

        point = UsagePoint(id="00000000000001", segment=UsagePointSegment["C5"])

        self.assertEqual(point.segment, UsagePointSegment.C5)

    def test_enum_when_fetching_objects(self):

        point = UsagePoint(id="00000000000000", segment=UsagePointSegment.C5)
        self.session.add(point)
        self.session.commit()

        # When retreived, field can be compared with the enum, but not a string

        self.assertEqual(point.segment, UsagePointSegment.C5)
        self.assertNotEqual(point.segment, "C5")

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

    def test_db_denies_call_for_an_out_of_scope_usage_point_when_consent_is_open(self):
        """
        Genius consent allows him to access any usage point, but his open consent
        still need to be associated with those.
        is_open is a flag telling that we can add the usage point to a the scope
        of a consent when it is open.
        """

        WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=self.genius_open_consent,
            user=self.genius,
            webservice="ConsultationMesures",
            called_at=date_local(2020, 1, 2),
        )

        with self.assertRaises(IntegrityError) as context:
            self.session.commit()

        self.assertTrue(
            'is not present in table "consents_usage_points"' in str(context.exception)
        )

    def test_db_allows_call_for_a_usage_point_when_consent_is_open(self):
        """
        Genius consent allows him to access any usage point, but his open consent
        still need to be associated with those.
        is_open is a flag telling that we can add the usage point to a the scope
        of a consent when it is open.
        """

        # consent_for helper will automatically add the usage point to consent
        # scope when it is open.
        consent = self.genius.consent_for(
            self.session, self.burns_usage_point, date_local(2020, 1, 2)
        )

        WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=consent,
            user=self.genius,
            webservice="ConsultationMesures",
            called_at=date_local(2020, 1, 2),
        )

        self.session.commit()

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

    def test_db_checked_webservices_call_ok(self):
        """
        Webservices calls are typically wrapped in a block that will fill
        its status to OK on success.
        """

        call = WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=self.burns_consent_to_sister,
            user=self.sister,
            webservice="ConsultationMesures",
        )

        self.assertIsNone(call.status)

        with CheckedWebserviceCall(call, self.session):
            # No error happen
            pass

        self.assertEqual(call.status, WebservicesCallStatus.OK)
        self.assertIsNone(call.error)
        self.assertTrue(call in self.session and not self.session.dirty)

    def test_db_checked_webservices_call_sge_error(self):
        """
        Webservices calls are typically wrapped in a block that will fill
        its status to FAILED on failure.
        """

        call = WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=self.burns_consent_to_sister,
            user=self.sister,
            webservice="ConsultationMesures",
        )

        with self.assertRaises(SgeError):
            with CheckedWebserviceCall(call, self.session):
                raise SgeError(
                    message="Une erreur technique est survenue", code="SGT500"
                )

        self.assertEqual(call.status, WebservicesCallStatus.FAILED)
        self.assertEqual(call.error, "SGT500: Une erreur technique est survenue")
        self.assertTrue(call in self.session and not self.session.dirty)

    def test_db_checked_webservices_call_integrity_error(self):

        call = WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=self.homer_consent_to_alice,
            user=self.sister,
            webservice="ConsultationMesures",
        )

        with self.assertRaises(IntegrityError):
            with CheckedWebserviceCall(call, self.session):
                self.assertFalse(True, "Webservices must not be called")

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

        self.assertTrue("is no longer valid" in str(context.exception))

    def test_db_can_subscribe_when_consent_is_valid(self):

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

    def test_db_can_notify_when_consent_is_valid(self):

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

    def test_multiple_calls_per_subscription_ok(self):

        # First, create the Subscription object,
        # keeps track of the request / intent

        subscription = Subscription(
            user=self.sister,
            usage_point=self.burns_usage_point,
            series_name="consumption/power/active/raw",
            consent=self.burns_consent_to_sister,
        )
        self.session.add(subscription)

        # Then keep track of associated SGE subscriptions calls

        # First call to enable load curve
        call = WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=self.burns_consent_to_sister,
            user=self.sister,
            webservice="CommandeCollectePublicationMesures-V3.0",
        )

        with CheckedWebserviceCall(call, self.session):
            subscription_params = WebservicesCallsSubscriptions(
                webservices_call=call,
                call_type=SubscriptionType.CONSUMPTION_CDC_ENABLE,
                expires_at=self.burns_consent_to_sister.expires_at,
                call_id=123,
            )
            self.session.add(subscription_params)
            subscription.webservices_calls.append(subscription_params)

        # Second call to subscribe to load curve
        call = WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=self.burns_consent_to_sister,
            user=self.sister,
            webservice="CommandeCollectePublicationMesures-V3.0",
        )

        with CheckedWebserviceCall(call, self.session):
            subscription_params = WebservicesCallsSubscriptions(
                webservices_call=call,
                call_type=SubscriptionType.CONSUMPTION_CDC_RAW,
                expires_at=self.burns_consent_to_sister.expires_at,
                call_id=124,
            )
            self.session.add(subscription_params)
            subscription.webservices_calls.append(subscription_params)

        self.session.commit()

        self.assertEqual(len(subscription.webservices_calls), 2)

    def test_multiple_subscriptions_per_call(self):

        # First, subscribe to consumption on peak hours only
        # This require a subscription call for all indices

        subscription = Subscription(
            user=self.sister,
            usage_point=self.burns_usage_point,
            series_name="consumption/energy/index/hp",
            consent=self.burns_consent_to_sister,
        )

        self.session.add(subscription)

        # Indices subscription have not been called yet

        existing_call_params = WebservicesCallsSubscriptions.find_existing(
            self.session,
            self.burns_usage_point.id,
            SubscriptionType.CONSUMPTION_IDX,
        )

        self.assertIsNone(existing_call_params)

        # Call indices subscription

        call = WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=self.burns_consent_to_sister,
            user=self.sister,
            webservice="CommandeCollectePublicationMesures-V3.0",
        )

        with CheckedWebserviceCall(call, self.session):
            subscription_params = WebservicesCallsSubscriptions(
                webservices_call=call,
                call_type=SubscriptionType.CONSUMPTION_IDX,
                expires_at=self.burns_consent_to_sister.expires_at,
                call_id=123,
            )
            self.session.add(subscription_params)
            subscription.webservices_calls.append(subscription_params)

        # Then subscribe to off-peak hours

        subscription = Subscription(
            user=self.sister,
            usage_point=self.burns_usage_point,
            series_name="consumption/energy/index/hc",
            consent=self.burns_consent_to_sister,
        )

        # Indices subscription have been called before, use it

        existing_call_params = WebservicesCallsSubscriptions.find_existing(
            self.session,
            self.burns_usage_point.id,
            SubscriptionType.CONSUMPTION_IDX,
        )

        self.assertEqual(existing_call_params.call_id, 123)

        subscription.webservices_calls.append(existing_call_params)

        # Given an call, we can find all subscriptions that depends on it
        self.assertEqual(len(existing_call_params.subscriptions), 2)

        self.session.commit()

    def test_subscription_call_cannot_expire_after_consent(self):

        call = WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=self.burns_consent_to_sister,
            user=self.sister,
            webservice="CommandeCollectePublicationMesures-V3.0",
        )

        with self.assertRaises(IntegrityError):
            with CheckedWebserviceCall(call, self.session):
                subscription_params = WebservicesCallsSubscriptions(
                    webservices_call=call,
                    call_type=SubscriptionType.CONSUMPTION_CDC_ENABLE,
                    expires_at=self.burns_consent_to_sister.expires_at
                    + dt.timedelta(days=1),
                    call_id=123,
                )
                self.session.add(subscription_params)

    def test_subscription_call_cleanup(self):

        # A call is used for two subscription

        sub1 = Subscription(
            user=self.sister,
            usage_point=self.burns_usage_point,
            series_name="consumption/energy/index/hp",
            consent=self.burns_consent_to_sister,
        )
        self.session.add(sub1)

        sub2 = Subscription(
            user=self.sister,
            usage_point=self.burns_usage_point,
            series_name="consumption/energy/index/hc",
            consent=self.burns_consent_to_sister,
        )
        self.session.add(sub2)

        call = WebservicesCall(
            usage_point=self.burns_usage_point,
            consent=self.burns_consent_to_sister,
            user=self.sister,
            webservice="CommandeCollectePublicationMesures-V3.0",
        )

        with CheckedWebserviceCall(call, self.session):
            subscription_params = WebservicesCallsSubscriptions(
                webservices_call=call,
                call_type=SubscriptionType.CONSUMPTION_IDX,
                expires_at=self.burns_consent_to_sister.expires_at,
                call_id=123,
            )
            sub1.webservices_calls.append(subscription_params)
            sub2.webservices_calls.append(subscription_params)

        self.session.commit()

        # No call is not used by any subscription
        self.assertEqual(
            0, WebservicesCallsSubscriptions.query_unused(self.session).count()
        )

        # When unsubscribing first
        # - unlink subscription from calls
        # - delete subscription
        # - cleanup calls

        sub1.webservices_calls[:] = []
        self.session.delete(sub1)

        # No call is not used by any subscription, nothing to cleanup
        self.assertEqual(
            0, WebservicesCallsSubscriptions.query_unused(self.session).count()
        )

        # Call is still in db
        self.assertEqual(1, self.session.query(WebservicesCallsSubscriptions).count())

        # Unsubscribe second

        sub2.webservices_calls[:] = []
        self.session.delete(sub2)

        # One call is no longer needed at this point, it should be cleaned up
        self.assertEqual(
            1, WebservicesCallsSubscriptions.query_unused(self.session).count()
        )

        for call in WebservicesCallsSubscriptions.query_unused(self.session):
            # Actually unsubscribe then delete
            self.session.delete(call)
            # FIXME transfer subscription consent if needed
            # if the consent used for subscription was used for the SGE call,
            # a wrong mimatching consent might be used

        # Call is no longer in db
        self.assertEqual(0, self.session.query(WebservicesCallsSubscriptions).count())


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
