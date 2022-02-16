import datetime as dt
import quoalise

class Authorizations:

    def __init__(self):
        self.authorizations = {}

    # TODO one authorization per user_id+usage_point_id for now, do we need more?
    def set(self, user_id, usage_point_id, authorization):
        if not user_id in self.authorizations:
            self.authorizations[user_id] = {}
        assert not usage_point_id in self.authorizations[user_id]
        self.authorizations[user_id][usage_point_id] = authorization

    def validate(self, user_id, usage_point_id, measurement_scope=None):

        allowed_usage_points = self.authorizations.get(user_id)
        if not allowed_usage_points:
            raise PermissionError(f"{user_id} is not allowed to access the proxy")

        authorization = allowed_usage_points.get(usage_point_id)
        if not authorization:
            raise PermissionError(
                f"{user_id} is not allowed to access usage point {usage_point_id}")

        if dt.date.today() < authorization.begins_at:
            raise PermissionError(
                f"{user_id} authorization for {usage_point_id} is not valid yet")

        if dt.date.today() > authorization.expires_at:
            raise PermissionError(
                f"{user_id} authorization for {usage_point_id} is no longer valid")

        if measurement_scope:
            if not authorization.measurement_scope:
                raise PermissionError(
                    f"{user_id} is not allower to access {usage_point_id} measurements")
            exclusions = authorization.measurement_scope.get_exclusions(measurement_scope)
            if exclusions:
                raise PermissionError(
                    f"{user_id} is not allowed to access {usage_point_id} {', '.join(exclusions)}")

    @classmethod
    def from_conf(cls, conf):
        authorizations = Authorizations()
        for user_id, auths in conf['allowed-users'].items():
            for auth in auths:
                authorization = Authorization.from_conf(auth)
                for usage_point_id in auth['usage_points']:
                    authorizations.set(user_id, usage_point_id, authorization)
        return authorizations

class Authorization:

    def __init__(self, begins_at, expires_at, measurement_scope=None):
        assert isinstance(begins_at, dt.date)
        assert isinstance(expires_at, dt.date)
        self.begins_at = begins_at
        self.expires_at = expires_at
        self.measurement_scope = measurement_scope

    @classmethod
    def from_conf(cls, conf):
        return cls(quoalise.parse_iso_date(conf['begins_at']),
                   quoalise.parse_iso_date(conf['expires_at']),
                   measurement_scope=MeasurementScope.from_conf(conf['measurements']))

class MeasurementScope:

    def __init__(self, details=False, consumption=False, production=False, history=None):
        self.details = details
        self.consumption = consumption
        self.production = production
        self.history = history

    @classmethod
    def from_conf(cls, conf):
        if conf is None:
            return None

        return cls(
            details=conf.get('details'),
            consumption=conf.get('consumption'),
            production=conf.get('production'),
            history=HistoryScope.from_conf(conf['history'])
        )

    def get_exclusions(self, other_scope):

        exclusions = []

        if other_scope.details and not self.details:
            exclusions.append("detailed measurements")

        if other_scope.consumption and not self.consumption:
            exclusions.append("consumption")

        if other_scope.production and not self.production:
            exclusions.append("production")

        if other_scope.history:
            if not self.history:
                exclusions.append("history")
            else:
                exclusions += self.history.get_exclusions(other_scope.history)

        return exclusions

class HistoryScope:

    def __init__(self, date_from=None, date_to=None):
        assert date_from is None or isinstance(date_from, dt.date)
        assert date_to is None or isinstance(date_to, dt.date)
        self.date_from = date_from
        self.date_to = date_to

    @classmethod
    def from_conf(cls, conf):
        if conf is None:
            return None
        date_from = conf.get('from')
        date_to = conf.get('to')
        date_from = quoalise.parse_iso_date(date_from) if date_from else None
        date_to = quoalise.parse_iso_date(date_to) if date_to else None
        return cls(date_from, date_to)

    def get_exclusions(self, other_scope):
        exclusions = []
        if self.date_to:
            if any([not other_scope.date_to,
                    other_scope.date_to and other_scope.date_to > self.date_to,
                    other_scope.date_from and other_scope.date_from > self.date_from]):
                exclusions.append(f"history after {self.date_to}")
        if self.date_from:
            if any([not other_scope.date_from,
                    other_scope.date_to and other_scope.date_to < self.date_from,
                    other_scope.date_from and other_scope.date_from < self.date_from]):
                exclusions.append(f"history before {self.date_from}")
        return exclusions