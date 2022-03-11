import os
import re
import enum
import glob
import datetime as dt

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
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text

Base = declarative_base()


class Migration(Base):
    __tablename__ = "migrations"

    version = Column(Integer, primary_key=True)
    applied_at = Column(DateTime)

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

    def __repr__(self):
        return f"<User(bare_jid='{self.bare_jid}')>"

    def consent_for(self, session, usage_point, call_date=dt.datetime.now()):

        if type(usage_point) == UsagePoint:
            usage_point = usage_point.id

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


class UsagePoint(Base):
    __tablename__ = "usage_points"

    id = Column(String(14), primary_key=True)

    webservices_calls = relationship("WebservicesCall", back_populates="usage_point")

    def __repr__(self):
        return f"<UsagePoint(id='{self.id}')>"


class Consent(Base):
    __tablename__ = "consents"

    id = Column(Integer, primary_key=True)
    issuer_name = Column(Text)
    issuer_type = Column(String())
    begins_at = Column(DateTime)
    expires_at = Column(DateTime)
    created_at = Column(DateTime, default=dt.datetime.now())

    usage_points = relationship("ConsentUsagePoint", cascade="all, delete-orphan")

    users = relationship(
        "User", secondary=consents_users_association, back_populates="consents"
    )

    webservices_calls = relationship("WebservicesCall", back_populates="consent")

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
    consent_begins_at = Column(DateTime)
    consent_expires_at = Column(DateTime)
    called_at = Column(DateTime, default=dt.datetime.now())

    status = Column(Enum(WebservicesCallStatus))
    error = Column(Text)

    user = relationship("User", back_populates="webservices_calls")
    usage_point = relationship("UsagePoint", back_populates="webservices_calls")
    consent = relationship("Consent", back_populates="webservices_calls")

    __table_args__ = (
        ForeignKeyConstraint(
            ["consent_id", "consent_begins_at", "consent_expires_at"],
            ["consents.id", "consents.begins_at", "consents.expires_at"],
        ),
        {},
    )

    def register(self, webservice, user_id, usage_point_id, date_from, date_to):
        pass
