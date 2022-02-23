import unittest
import glob
import os
import re

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sgeproxy.db import User, UsagePoint, Consent, WebservicesCall

import datetime as dt

# CREATE DATABASE sgeproxy_test;
# CREATE USER sgeproxy_test WITH ENCRYPTED PASSWORD 'S77RiVz6FVyJhbzb';
# GRANT ALL PRIVILEGES ON DATABASE sgeproxy_test TO sgeproxy_test;
# ALTER SCHEMA public OWNER TO sgeproxy_test;

DB_URL = "postgresql://sgeproxy_test:S77RiVz6FVyJhbzb@127.0.0.1:5433/sgeproxy_test"


class TestDbMigrations(unittest.TestCase):
    def test_migrations_update_in_db_version_correctly(self):
        """
        Check that all migrations can be applied from an empty database,
        setting the current state (version) correctly.
        """

        engine = create_engine(DB_URL)

        with engine.connect() as con:

            con.execute("DROP SCHEMA public CASCADE")
            con.execute("CREATE SCHEMA public")

            for sql_file in sorted(glob.glob("migrations/*.sql")):

                filename = os.path.basename(sql_file)
                result = re.match(r"^(\d{4})_.*\.sql", filename)

                self.assertTrue(result)

                version = int(result.group(1))

                with open(sql_file) as f:
                    query = text(f.read())
                    con.execute(query)

                query = text(
                    "SELECT version FROM migrations ORDER BY applied_at DESC LIMIT 1"
                )
                result = con.execute(query)
                row = result.fetchone()
                self.assertEqual(version, row[0])


class TestDbConsents(unittest.TestCase):
    def setUp(self):

        engine = create_engine(DB_URL)

        with engine.connect() as con:

            con.execute("DROP SCHEMA public CASCADE")
            con.execute("CREATE SCHEMA public")

            for sql_file in sorted(glob.glob("migrations/*.sql")):
                with open(sql_file) as f:
                    query = text(f.read())
                    con.execute(query)

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
            begins_at=dt.datetime(2020, 1, 1),
            expires_at=dt.datetime(2021, 1, 1),
        )
        self.homer_consent_to_alice.usage_points.append(self.homer_usage_point)
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
            begins_at=dt.datetime(2020, 1, 1),
            expires_at=dt.datetime(2021, 3, 1),
        )
        self.burns_consent_to_sister.usage_points.append(self.burns_usage_point)
        self.session.add(self.burns_consent_to_sister)

        self.sister.consents.append(self.burns_consent_to_sister)

        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_db_allows_call_within_consent_scope(self):

        WebservicesCall(
            usage_point=self.homer_usage_point,
            consent=self.homer_consent_to_alice,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=dt.datetime(2020, 1, 2),
        )

        self.session.commit()

    def test_db_denies_call_with_no_consent(self):

        WebservicesCall(
            usage_point=self.homer_usage_point,
            consent=None,
            user=self.alice,
            webservice="ConsultationMesures",
            called_at=dt.datetime(2020, 1, 2),
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
            called_at=dt.datetime(2020, 1, 2),
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
            called_at=dt.datetime(2020, 1, 2),
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
            called_at=dt.datetime(2020, 1, 2),
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
            called_at=dt.datetime(2020, 1, 2),
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
            called_at=dt.datetime(2019, 1, 2),
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
            called_at=dt.datetime(2022, 1, 2),
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
            called_at=dt.datetime(2020, 1, 2),
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
            called_at=dt.datetime(2020, 1, 2),
        )
        self.session.add(call)

        self.homer_consent_to_alice.usage_points.remove(self.homer_usage_point)

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
            called_at=dt.datetime(2020, 1, 2),
        )
        self.session.add(call)

        self.homer_consent_to_alice.begins_at = dt.datetime(2020, 1, 3)

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_db_find_consents(self):

        consent = self.alice.consent_for(
            self.session, self.homer_usage_point, dt.datetime(2020, 1, 3)
        )
        self.assertEqual(consent, self.homer_consent_to_alice)

        consent = self.sister.consent_for(
            self.session, self.burns_usage_point, dt.datetime(2020, 1, 3)
        )
        self.assertEqual(consent, self.burns_consent_to_sister)

    def test_db_throws_exception_when_no_consent_given_for_usage_point(self):

        with self.assertRaises(PermissionError) as context:
            self.alice.consent_for(
                self.session, self.burns_usage_point, dt.datetime(2020, 1, 3)
            )

        with self.assertRaises(PermissionError) as context:
            self.sister.consent_for(
                self.session, self.homer_usage_point, dt.datetime(2020, 1, 3)
            )

        with self.assertRaises(PermissionError) as context:
            self.sister.consent_for(
                self.session, "00000000000000", dt.datetime(2020, 1, 3)
            )

        self.assertTrue("No consent registered for" in str(context.exception))

    def test_db_throws_exception_when_no_consent_has_not_started_yet(self):

        with self.assertRaises(PermissionError) as context:
            self.alice.consent_for(
                self.session, self.homer_usage_point, dt.datetime(2019, 12, 31)
            )

        with self.assertRaises(PermissionError) as context:
            self.sister.consent_for(
                self.session, self.burns_usage_point, dt.datetime(2019, 12, 31)
            )

        self.assertTrue("is not valid yet" in str(context.exception))

    def test_db_throws_exception_when_consent_has_expired(self):

        with self.assertRaises(PermissionError) as context:
            self.alice.consent_for(
                self.session, self.homer_usage_point, dt.datetime(2021, 1, 1)
            )

        with self.assertRaises(PermissionError) as context:
            self.sister.consent_for(
                self.session, self.burns_usage_point, dt.datetime(2021, 3, 1)
            )

        self.assertTrue("is no longer valid" in str(context.exception))


if __name__ == "__main__":

    import sys
    import argparse
    import logging

    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO")
    args, unittest_args = parser.parse_known_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    unittest.main(argv=[sys.argv[0]] + unittest_args)
