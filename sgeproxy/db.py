import os
import re
import enum
import glob
import datetime as dt
from typing import Any

from sqlalchemy import (
    Integer,
    String,
    Text,
    DateTime,
    Enum,
    Table,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    TypeDecorator,
    Boolean,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session
from sqlalchemy.sql import text, exists

Base: Any = declarative_base()


def date_local(year, month, day):
    return dt.datetime(year, month, day, tzinfo=dt.timezone.utc).astimezone()


def now_local():
    return dt.datetime.now(dt.timezone.utc).astimezone()


class TZDateTime(TypeDecorator):
    """
    Ensures date fields are stored with their timezone to avoid errors.
    Postgresql will keep those with TIMESTAMP WITH TIME ZONE fields.
    """

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            if not value.tzinfo:
                raise TypeError("tzinfo is required")
        return value


class Migration(Base):
    __tablename__ = "migrations"

    version = Column(Integer, primary_key=True)
    applied_at = Column(TZDateTime)

    def __repr__(self):
        return f"<Migration(version='{self.version}')>"

    @staticmethod
    def deployed_version(connection):
        # Return None if table does not exist
        query = text("SELECT to_regclass('migrations')")
        if connection.execute(query).scalar() is None:
            return None
        # Else return the highest version
        query = text("SELECT MAX(version) FROM migrations")
        return connection.execute(query).scalar()

    @staticmethod
    def files():
        my_dir = os.path.dirname(__file__)
        files_glob = os.path.join(my_dir, "..", "migrations", "*.sql")
        migrations = []

        for sql_file in sorted(glob.glob(files_glob)):
            filename = os.path.basename(sql_file)
            result = re.match(r"^(\d{4})_.*\.sql", filename)
            if not result:
                raise RuntimeError(f"Unexpected migration file name {filename}")

            version = int(result.group(1))
            migrations.append((version, sql_file))

        return migrations

    @classmethod
    def migrate(cls, connection):
        deployed_version = cls.deployed_version(connection)

        for version, sql_file in cls.files():
            if deployed_version is not None and deployed_version >= version:
                continue

            with open(sql_file) as f:
                query = text(f.read())
                connection.execute(query)

            if cls.deployed_version(connection) != version:
                raise RuntimeError("Unexpected deployed version after migration")


class ConsentUsagePoint(Base):
    __tablename__ = "consents_usage_points"

    consent_id = Column(ForeignKey("consents.id"), primary_key=True)
    usage_point_id = Column(ForeignKey("usage_points.id"), primary_key=True)
    comment = Column(Text)

    usage_point = relationship("UsagePoint")


consents_users_association = Table(
    "consents_users",
    Base.metadata,
    Column("consent_id", ForeignKey("consents.id"), primary_key=True),
    Column("user_id", ForeignKey("users.bare_jid"), primary_key=True),
)


class User(Base):
    __tablename__ = "users"

    bare_jid = Column(String(2049), primary_key=True)

    consents = relationship(
        "Consent", secondary=consents_users_association, back_populates="users"
    )

    webservices_calls = relationship("WebservicesCall", back_populates="user")

    subscriptions = relationship("Subscription", back_populates="user")

    def __repr__(self):
        return f"<User(bare_jid='{self.bare_jid}')>"

    def consent_for(self, session, usage_point, call_date=now_local()):

        if type(usage_point) == UsagePoint:
            usage_point = usage_point.id

        try:
            return self.restricted_consent_for(session, usage_point, call_date)
        except PermissionError:
            # No restricted consent can be used for this point,
            # check if there is an open one.
            open_consent = (
                session.query(Consent)
                .join(consents_users_association)
                .join(User)
                .filter(consents_users_association.c.user_id == self.bare_jid)
                .filter(Consent.is_open)
                .filter(Consent.begins_at <= call_date)
                .filter(Consent.expires_at > call_date)
            ).first()

            if open_consent is not None:
                open_consent.usage_points.append(
                    ConsentUsagePoint(
                        usage_point=UsagePoint.find_or_create(session, usage_point)
                    )
                )
                return open_consent

            raise

    # TODO(cyril) get session from object
    def restricted_consent_for(self, session, usage_point, call_date):

        query = (
            session.query(Consent)
            .join(consents_users_association)
            .join(User)
            .filter(consents_users_association.c.user_id == self.bare_jid)
            .join(ConsentUsagePoint)
            .filter(ConsentUsagePoint.usage_point_id == usage_point)
        )

        if query.limit(1).count() == 0:
            raise PermissionError(f"No consent registered for {usage_point}")

        query = query.filter(call_date >= Consent.begins_at)

        if query.limit(1).count() == 0:
            raise PermissionError(
                f"Consent registered for {usage_point} is not valid yet"
            )

        query = query.filter(call_date < Consent.expires_at)

        if query.limit(1).count() == 0:
            raise PermissionError(
                f"Consent registered for {usage_point} is no longer valid"
            )

        return query.first()


class UsagePointSegment(enum.Enum):
    C1 = "C1"
    C2 = "C2"
    C3 = "C3"
    C4 = "C4"
    C5 = "C5"
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"
    P4 = "P4"


class UsagePoint(Base):
    __tablename__ = "usage_points"

    id = Column(String(14), primary_key=True)

    segment = Column(Enum(UsagePointSegment, name="usage_point_segment"))
    service_level = Column(Integer)

    webservices_calls = relationship("WebservicesCall", back_populates="usage_point")

    subscriptions = relationship("Subscription", back_populates="usage_point")

    def __repr__(self):
        return f"<UsagePoint(id='{self.id}')>"

    @staticmethod
    def find_or_create(session: Session, usage_point_id: str):
        found = session.get(UsagePoint, usage_point_id)
        if found is not None:
            return found

        created = UsagePoint(id=usage_point_id)
        session.add(created)
        return created


class ConsentIssuerType(enum.Enum):
    INDIVIDUAL = "INDIVIDUAL"
    COMPANY = "COMPANY"


class Consent(Base):
    __tablename__ = "consents"

    id = Column(Integer, primary_key=True)
    issuer_name = Column(Text)
    issuer_type = Column(Enum(ConsentIssuerType, name="consent_issuer_type"))
    is_open = Column(Boolean, default=False)
    begins_at = Column(TZDateTime)
    expires_at = Column(TZDateTime)
    created_at = Column(TZDateTime, default=now_local())

    usage_points = relationship("ConsentUsagePoint", cascade="all, delete-orphan")

    users = relationship(
        "User", secondary=consents_users_association, back_populates="consents"
    )

    webservices_calls = relationship("WebservicesCall", back_populates="consent")

    subscriptions = relationship("Subscription", back_populates="consent")

    def __repr__(self):
        return f"<Authorization(id='{self.id}', issuer_name='{self.issuer_name}')>"


class WebservicesCallStatus(enum.Enum):
    OK = "OK"
    FAILED = "FAILED"


class WebservicesCall(Base):
    __tablename__ = "webservices_calls"

    id = Column(Integer, primary_key=True)
    webservice = Column(String(100))
    usage_point_id = Column(String(14), ForeignKey("usage_points.id"))
    user_id = Column(String(2049), ForeignKey("users.bare_jid"))
    consent_id = Column(Integer)
    consent_begins_at = Column(TZDateTime)
    consent_expires_at = Column(TZDateTime)
    called_at = Column(TZDateTime, default=now_local())

    status = Column(Enum(WebservicesCallStatus, name="webservices_call_status"))
    error = Column(Text)

    user = relationship("User", back_populates="webservices_calls")
    usage_point = relationship("UsagePoint", back_populates="webservices_calls")
    consent = relationship("Consent", back_populates="webservices_calls")

    __table_args__: Any = (
        ForeignKeyConstraint(
            ["consent_id", "consent_begins_at", "consent_expires_at"],
            ["consents.id", "consents.begins_at", "consents.expires_at"],
        ),
        {},
    )


class CheckedWebserviceCall:
    """
    Prevents to make a not allowed call.

    - add the call to the session and commit
    - if it raises an integrity error
      - do not exectute the enclosed statement (meant to execute the actual
        webservice call)
    - else exectute the enclosed statement
      - if it raises an exception, set the call status to failed and fill the
        error field
      - else set the call status to ok
      - commit
    """

    def __init__(
        self,
        call,
        db_session: Session,
    ):
        self.call = call
        self.db_session = db_session

    def __enter__(self):
        self.call.called_at = now_local()
        self.db_session.add(self.call)
        self.db_session.commit()

    def __exit__(self, ex_type, ex_val, tb):
        if ex_type is None:
            self.call.status = WebservicesCallStatus.OK
        else:
            self.call.status = WebservicesCallStatus.FAILED
            self.call.error = str(ex_val)

        self.db_session.commit()

        # Raise exception
        return False


subscriptions_calls_association = Table(
    "subscriptions_calls",
    Base.metadata,
    Column("subscription_id", ForeignKey("subscriptions.id"), primary_key=True),
    Column(
        "webservices_calls_subscriptions_id",
        ForeignKey("webservices_calls_subscriptions.id"),
        primary_key=True,
    ),
)


class SubscriptionStatus(enum.Enum):
    OK = "OK"
    FAILED = "FAILED"


class Subscription(Base):
    __tablename__ = "subscriptions"

    id = Column(Integer, primary_key=True)
    user_id = Column(String(2049), ForeignKey("users.bare_jid"))
    series_name = Column(Text)
    subscribed_at = Column(TZDateTime, default=now_local())
    notified_at = Column(TZDateTime)

    usage_point_id = Column(String(14), ForeignKey("usage_points.id"))
    consent_id = Column(Integer)
    consent_begins_at = Column(TZDateTime)
    consent_expires_at = Column(TZDateTime)

    status = Column(Enum(SubscriptionStatus, name="subscription_status"))
    error = Column(Text)

    user = relationship("User", back_populates="subscriptions")
    usage_point = relationship("UsagePoint", back_populates="subscriptions")
    consent = relationship("Consent", back_populates="subscriptions")

    webservices_calls = relationship(
        "WebservicesCallsSubscriptions",
        secondary=subscriptions_calls_association,
        back_populates="subscriptions",
    )

    __table_args__: Any = (
        ForeignKeyConstraint(
            ["consent_id", "consent_begins_at", "consent_expires_at"],
            ["consents.id", "consents.begins_at", "consents.expires_at"],
        ),
        {},
    )

    def notification_checks(self, notified_at=now_local()):
        return SubscriptionNotificationContext(self, notified_at)

    # Tell which calls will no longer be valid for this subscription at the given date
    def expired_calls(self, now=now_local()):
        if self.series_name == "consumption/power/active/raw":
            expired_calls = {
                SubscriptionType.CONSUMPTION_CDC_ENABLE,
                SubscriptionType.CONSUMPTION_CDC_RAW,
            }
        elif self.series_name in [
            "consumption/energy/active/index",
            "consumption/power/apparent/max",
        ]:
            expired_calls = {SubscriptionType.CONSUMPTION_IDX}
        else:
            raise ValueError("Unexpected subscription type")

        for call in self.webservices_calls:
            if call.call_type in expired_calls and call.expires_at > now:
                expired_calls.remove(call.call_type)
            if not expired_calls:
                break
        return expired_calls


class SubscriptionNotificationContext:
    def __init__(self, subscription, notified_at):
        self.date = notified_at
        self.sub = subscription
        self.session = Session.object_session(self.sub)

    def __enter__(self):
        self.sub.status = None
        self.sub.notified_at = self.date
        self.session.commit()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.sub.status = SubscriptionStatus.OK
        else:
            self.sub.status = SubscriptionStatus.FAILED
        self.session.commit()


class SubscriptionType(enum.Enum):
    CONSUMPTION_IDX = "CONSUMPTION_IDX"
    CONSUMPTION_CDC_RAW = "CONSUMPTION_CDC_RAW"
    CONSUMPTION_CDC_CORRECTED = "CONSUMPTION_CDC_CORRECTED"
    CONSUMPTION_CDC_ENABLE = "CONSUMPTION_CDC_ENABLE"
    PRODUCTION_IDX = "PRODUCTION_IDX"
    PRODUCTION_CDC_RAW = "PRODUCTION_CDC_RAW"
    PRODUCTION_CDC_CORRECTED = "PRODUCTION_CDC_CORRECTED"
    PRODUCTION_CDC_ENABLE = "PRODUCTION_CDC_ENABLE"


class WebservicesCallsSubscriptions(Base):
    __tablename__ = "webservices_calls_subscriptions"

    id = Column(Integer, primary_key=True)

    webservices_call_id = Column(Integer)
    consent_expires_at = Column(TZDateTime)

    call_type = Column(Enum(SubscriptionType, name="subscription_type"))

    expires_at = Column(TZDateTime)
    call_id = Column(Integer)

    # webservices_call = relationship("WebservicesCall",
    # back_populates="subscription_call")
    webservices_call = relationship("WebservicesCall")

    subscriptions = relationship(
        "Subscription",
        secondary=subscriptions_calls_association,
        back_populates="webservices_calls",
    )

    __table_args__: Any = (
        ForeignKeyConstraint(
            ["webservices_call_id", "consent_expires_at"],
            ["webservices_calls.id", "webservices_calls.consent_expires_at"],
        ),
        {},
    )

    @staticmethod
    def query_unused(session):
        stmt = exists().where(
            subscriptions_calls_association.c.webservices_calls_subscriptions_id
            == WebservicesCallsSubscriptions.id
        )
        return session.query(WebservicesCallsSubscriptions).filter(~stmt)

    @staticmethod
    def find_existing(
        session,
        usage_point_id: str,
        call_type: SubscriptionType,
        call_date=now_local(),
    ):
        query = (
            session.query(WebservicesCallsSubscriptions)
            .join(subscriptions_calls_association)
            .join(WebservicesCall)
            .filter(WebservicesCall.usage_point_id == usage_point_id)
            .filter(WebservicesCallsSubscriptions.call_type == call_type)
            .filter(WebservicesCallsSubscriptions.expires_at > call_date)
        )
        return query.first()
