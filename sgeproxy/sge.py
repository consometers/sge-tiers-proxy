#!/usr/bin/env python3

import datetime as dt
import zoneinfo
from typing import Any, Dict, Optional, Type
import inspect
from sgeproxy.metadata_enedis import (
    MetadataEnedisConsumptionPowerActiveRaw,
    MetadataEnedisConsumptionEnergyActive,
    MetadataEnedisProductionPowerActiveRaw,
    MetadataEnedisProductionEnergyActive,
)
from sgeproxy.metadata import SamplingInterval
import quoalise.data
import suds

import lowatt_enedis
import lowatt_enedis.services

# TODO move to quoalise
import re
from slixmpp.xmlstream import ElementBase

import sgeproxy.config
from sgeproxy.db import SubscriptionType, now_local

PARIS_TZ = zoneinfo.ZoneInfo("Europe/Paris")


class SgeError(Exception):
    def __init__(self, message: str, code: Optional[str] = None) -> None:
        self.message = message
        self.code = code

    def __str__(self) -> str:
        if self.code is not None:
            return f"{self.code}: {self.message}"
        else:
            return self.message


class SgeErrorConverter:
    """
    Transform different inconsistant error types from SGE API to an SgeError
    """

    def __enter__(self):
        pass

    def __exit__(self, ex_type, ex_val, tb):
        if ex_type is suds.WebFault:
            res = ex_val.fault.detail.erreur.resultat
            raise SgeError(res.value, res._code)
        if ex_type is Exception:
            # Suds client does not seem to handle HTTP errors very well,
            # it ends up raising Exception((503, 'Service Unavailable'))
            if (
                len(ex_val.args) == 1
                and len(ex_val.args[0]) == 2
                and type(ex_val.args[0][0]) == int
            ):
                code = str(ex_val.args[0][0])
                reason = str(ex_val.args[0][1])
                raise SgeError(reason, code)

        # Raise exception as-is if not handled above
        return False


class MeasurementApiSpec:
    def __init__(
        self,
        params: Dict[str, Optional[str]],
        metadata: Type[Any],
    ):
        self.params = params
        self.metadata = metadata


class DetailedMeasurementsV3:

    SERVICE_NAME = "ConsultationMesuresDetaillees-v3.0"

    MEASUREMENTS: Dict[str, MeasurementApiSpec] = {
        # -------------------------
        # Courbes au pas enregistré
        #
        # 7 jours consécutifs maximum dans les 24 derniers mois par
        # rapport à la date du jour, limités à la dernière mise en
        # service.
        #
        # Courbe de puissance active consommée brute
        "consumption/power/active/raw": MeasurementApiSpec(
            params={
                "initiateurLogin": None,
                "pointId": None,
                "mesuresTypeCode": "COURBE",
                "grandeurPhysique": "PA",
                "dateDebut": None,
                "dateFin": None,
                "mesuresCorrigees": "false",
                "sens": "SOUTIRAGE",
                "cadreAcces": "ACCORD_CLIENT",
            },
            metadata=MetadataEnedisConsumptionPowerActiveRaw,
        ),
        # Courbe de puissance active consommée corrigées
        "consumption/power/active/corrected": MeasurementApiSpec(
            params={
                "initiateurLogin": None,
                "pointId": None,
                "mesuresTypeCode": "COURBE",
                "grandeurPhysique": "PA",
                "dateDebut": None,
                "dateFin": None,
                "mesuresCorrigees": "true",
                "sens": "SOUTIRAGE",
                "cadreAcces": "ACCORD_CLIENT",
            },
            metadata=MetadataEnedisConsumptionPowerActiveRaw,
        ),
        # Courbe de puissance active produite brute
        # P1-P4
        "production/power/active/raw": MeasurementApiSpec(
            params={
                "initiateurLogin": None,
                "pointId": None,
                "mesuresTypeCode": "COURBE",
                "grandeurPhysique": "PA",
                "dateDebut": None,
                "dateFin": None,
                "mesuresCorrigees": "false",
                "sens": "INJECTION",
                "cadreAcces": "ACCORD_CLIENT",
            },
            metadata=MetadataEnedisProductionPowerActiveRaw,
        ),
        # Courbe de puissance active produite corrigées
        # P1-P3
        "production/power/active/corrected": MeasurementApiSpec(
            params={
                "initiateurLogin": None,
                "pointId": None,
                "mesuresTypeCode": "COURBE",
                "grandeurPhysique": "PA",
                "dateDebut": None,
                "dateFin": None,
                "mesuresCorrigees": "true",
                "sens": "INJECTION",
                "cadreAcces": "ACCORD_CLIENT",
            },
            metadata=MetadataEnedisProductionPowerActiveRaw,
        ),
        # Consommations globales quotidiennes
        # C5
        "consumption/energy/active/daily": MeasurementApiSpec(
            params={
                "initiateurLogin": None,
                "pointId": None,
                "mesuresTypeCode": "ENERGIE",
                "grandeurPhysique": "EA",
                "dateDebut": None,
                "dateFin": None,
                "mesuresCorrigees": "false",
                "sens": "SOUTIRAGE",
                "cadreAcces": "ACCORD_CLIENT",
            },
            metadata=MetadataEnedisConsumptionEnergyActive,
        ),
        # Productions globales quotidiennes
        # P4
        "production/energy/active/daily": MeasurementApiSpec(
            params={
                "initiateurLogin": None,
                "pointId": None,
                "mesuresTypeCode": "ENERGIE",
                "grandeurPhysique": "EA",
                "dateDebut": None,
                "dateFin": None,
                "mesuresCorrigees": "false",
                "sens": "INJECTION",
                "cadreAcces": "ACCORD_CLIENT",
            },
            metadata=MetadataEnedisProductionEnergyActive,
        ),
    }

    def __init__(self, credentials: sgeproxy.config.File):

        self.login = credentials["login"]
        certificate = credentials.abspath(credentials["certificate"])
        private_key = credentials.abspath(credentials["private-key"])
        homologation = credentials["environment"] != "production"

        self.client = lowatt_enedis.get_client(
            self.SERVICE_NAME, certificate, private_key, homologation
        )

    # end date included (!= SGE API)
    def get_measurements(
        self,
        name: str,
        usage_point_id: str,
        start_time: dt.datetime,
        end_time: dt.datetime,
    ) -> Any:

        # SGE works with dates, on french timezone
        # It only allows to get complete days
        # TODO(cyril) handle times, it is zeroed out for now
        start_date = start_time.astimezone(PARIS_TZ).date()
        end_date = end_time.astimezone(PARIS_TZ).date()

        measurement = self.MEASUREMENTS.get(name)

        if measurement is None:
            raise ValueError(f"{name} measurement is not known")

        if len(usage_point_id) != 14:
            raise ValueError(
                f"Usage point id {usage_point_id} must consist of 14 digits"
            )

        demande = measurement.params.copy()
        demande.update(
            {
                "initiateurLogin": self.login,
                "pointId": usage_point_id,
                "dateDebut": start_date.isoformat(),
                "dateFin": end_date.isoformat(),
            }
        )

        with SgeErrorConverter():
            resp = self.client.service.consulterMesuresDetailleesV3(demande)

        meta_sig = inspect.signature(measurement.metadata.__init__)
        if "sampling_interval" in meta_sig.parameters:
            sampling_interval = SamplingInterval(resp.grandeur[0].points[0].p)
            meta = measurement.metadata(usage_point_id, sampling_interval)
        else:
            meta = measurement.metadata(usage_point_id)

        m = re.match(r"^PT(\d+)M$", meta.measurement.sampling_interval.value)
        if m:
            # load curve is stamped by SGE API at the end of measurement periods
            # TODO, is this true for all segments? (see HDM)
            time_offset = int(m.group(1)) * 60
        elif meta.measurement.sampling_interval.value == "P1D":
            # daily data is stamped at the begining of the day
            time_offset = 0
        else:
            raise SgeError(
                "Unexpected time period: " + meta.measurement.sampling_interval.value
            )

        assert resp.grandeur[0].unite == meta.measurement.unit.value

        records = []

        for meas in resp.grandeur[0].points:
            v = int(meas.v)
            # t = dt.datetime.strptime(meas.d, "%Y-%m-%d %H:%M:%S")
            t = dt.datetime.fromisoformat(meas.d)
            t = t.replace(tzinfo=PARIS_TZ)
            t -= dt.timedelta(seconds=time_offset)
            if "p" in dir(meas):
                # TODO fails when sampling interval changed (it can happen
                # during the day). Send with different meta instead.
                # check journal on 2023-10-27
                # assert meas.p == measurement_meta.attrib["sampling-interval"]
                pass
            record = quoalise.data.Record(
                name=f"urn:dev:prm:{usage_point_id}_{name}",
                time=t,
                unit=meta.measurement.unit.value,
                value=v,
            )
            records.append(record)

        data = quoalise.data.Data(
            metadata=quoalise.data.Metadata(meta.to_dict()), records=records
        )

        class Quoalise(ElementBase):
            name = "quoalise"
            namespace = "urn:quoalise:0"

        quoalise_element = Quoalise()
        quoalise_element.append(data.to_xml())
        return quoalise_element


class TechnicalData:

    SERVICE_NAME = "ConsultationDonneesTechniquesContractuelles-v1.0"

    def __init__(self, credentials: sgeproxy.config.File):

        self.login = credentials["login"]
        certificate = credentials.abspath(credentials["certificate"])
        private_key = credentials.abspath(credentials["private-key"])
        homologation = credentials["environment"] != "production"

        self.client = lowatt_enedis.get_client(
            self.SERVICE_NAME,
            certificate,
            private_key,
            homologation,
        )

    def get_technical_data(
        self,
        usage_point_id: str,
    ) -> Any:

        with SgeErrorConverter():
            resp = self.client.service.consulterDonneesTechniquesContractuelles(
                pointId=usage_point_id,
                loginUtilisateur=self.login,
                autorisationClient="true",
            )

        return resp


def sub_params(updates):
    # Request is considered malformed when params are not ordered
    sorted_params = {
        "dateDebut": None,
        "dateFin": None,
        "declarationAccordClient": {"accord": "true"},
        "mesuresTypeCode": None,
        "soutirage": "true",
        "injection": "false",
        "mesuresPas": None,
        "mesuresCorrigees": "false",
        "transmissionRecurrente": "true",
        "periodiciteTransmission": "P1D",
    }
    sorted_params.update(updates)
    return sorted_params


class Subscribe:

    SERVICE_NAME = "CommandeCollectePublicationMesures-v3.0"

    PARAMS = {
        SubscriptionType.CONSUMPTION_IDX: sub_params(
            {
                "mesuresTypeCode": "IDX",
                "soutirage": "true",
                "injection": "false",
            }
        ),
        SubscriptionType.CONSUMPTION_CDC_RAW: sub_params(
            {
                "mesuresTypeCode": "CDC",
                "soutirage": "true",
                "injection": "false",
            }
        ),
        SubscriptionType.CONSUMPTION_CDC_CORRECTED: sub_params(
            {
                "mesuresTypeCode": "CDC",
                "soutirage": "true",
                "injection": "false",
            }
        ),
        SubscriptionType.CONSUMPTION_CDC_ENABLE: sub_params(
            {
                "mesuresTypeCode": "CDC",
                "soutirage": "true",
                "injection": "false",
                "transmissionRecurrente": "false",
            }
        ),
        SubscriptionType.PRODUCTION_IDX: sub_params(
            {
                "mesuresTypeCode": "IDX",
                "soutirage": "false",
                "injection": "true",
            }
        ),
        SubscriptionType.PRODUCTION_CDC_RAW: sub_params(
            {
                "mesuresTypeCode": "CDC",
                "soutirage": "false",
                "injection": "true",
            }
        ),
        SubscriptionType.PRODUCTION_CDC_CORRECTED: sub_params(
            {
                "mesuresTypeCode": "CDC",
                "soutirage": "false",
                "injection": "true",
                "mesuresCorrigees": "true",
            }
        ),
        SubscriptionType.PRODUCTION_CDC_ENABLE: sub_params(
            {
                "mesuresTypeCode": "CDC",
                "soutirage": "false",
                "injection": "true",
                "transmissionRecurrente": "false",
            }
        ),
    }

    def __init__(self, credentials: sgeproxy.config.File):

        self.login = credentials["login"]
        self.contract = credentials["contract-id"]
        certificate = credentials.abspath(credentials["certificate"])
        private_key = credentials.abspath(credentials["private-key"])
        homologation = credentials["environment"] != "production"

        self.client = lowatt_enedis.get_client(
            self.SERVICE_NAME,
            certificate,
            private_key,
            homologation,
        )

    def subscribe(
        self,
        usage_point_id: str,
        call_type: SubscriptionType,
        expires_at: dt.datetime,
        is_linky: bool,
        consent_issuer_is_company: bool,
        consent_issuer_name,
    ) -> Any:

        params = self.PARAMS[call_type]
        params["dateDebut"] = now_local().astimezone(PARIS_TZ).date().isoformat()
        params["dateFin"] = expires_at.astimezone(PARIS_TZ).date().isoformat()

        if consent_issuer_is_company:
            params["declarationAccordClient"]["personneMorale"] = {
                "denominationSociale": consent_issuer_name
            }
        else:
            params["declarationAccordClient"]["personnePhysique"] = {
                "nom": consent_issuer_name
            }

        if params["mesuresTypeCode"] == "IDX":
            params["mesuresPas"] = "P1D"
        elif is_linky:
            params["mesuresPas"] = "PT30M"
        else:
            params["mesuresPas"] = "PT10M"

        demande = {
            "donneesGenerales": {
                "objetCode": "AME",
                "pointId": usage_point_id,
                "initiateurLogin": self.login,
                "contratId": self.contract,
            },
            "accesMesures": params,
        }

        with SgeErrorConverter():
            resp = self.client.service.commanderCollectePublicationMesures(demande)

        return resp


class Unsubscribe:

    SERVICE_NAME = "CommandeArretServiceSouscritMesures-v1.0"

    def __init__(self, credentials: sgeproxy.config.File):

        self.login = credentials["login"]
        self.contract = credentials["contract-id"]
        certificate = credentials.abspath(credentials["certificate"])
        private_key = credentials.abspath(credentials["private-key"])
        homologation = credentials["environment"] != "production"

        self.client = lowatt_enedis.get_client(
            self.SERVICE_NAME,
            certificate,
            private_key,
            homologation,
        )

    def unsubscribe(
        self,
        usage_point_id: str,
        call_id: int,
    ) -> Any:

        demande = {
            "donneesGenerales": {
                # "refExterne": "",
                "objetCode": "ASS",
                "pointId": usage_point_id,
                "initiateurLogin": self.login,
                "contratId": self.contract,
            },
            "arretServiceSourscrit": {"serviceSouscritId": call_id},
        }

        with SgeErrorConverter():
            resp = self.client.service.commanderArretServiceSouscritMesures(demande)

        return resp
