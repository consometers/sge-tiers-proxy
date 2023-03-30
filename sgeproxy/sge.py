#!/usr/bin/env python3

import datetime as dt
from pytz import timezone
from typing import Any, Dict, Optional, Set
import suds

import lowatt_enedis
import lowatt_enedis.services

# TODO move to quoalise
import re
from slixmpp.xmlstream import ElementBase, ET
import pytz

import sgeproxy.config
from sgeproxy.db import SubscriptionType, now_local

PARIS_TZ = timezone("Europe/Paris")


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
        availability: Set[str],
        metadata: Dict[str, Any],
    ):
        self.params = params
        self.availability = availability
        self.metadata = metadata


class DetailedMeasurements:

    # TODO(cyril) ensure Python >= 3.7 to preserve dict order

    SERVICE_NAME = "ConsultationMesuresDetaillees-v2.0"

    MEASUREMENTS: Dict[str, MeasurementApiSpec] = {
        # -------------------------
        # Courbes au pas enregistré
        #
        # 7 jours consécutifs maximum dans les 24 derniers mois par rapport
        # à la date du jour, limités à la dernière mise en service.
        #
        # Courbe de puissance active consommée brute
        "consumption/power/active/raw": MeasurementApiSpec(
            params={
                "initiateurLogin": None,
                "pointId": None,
                "mesuresTypeCode": "COURBE",
                "grandeurPhysique": "PA",
                "soutirage": "true",
                "injection": "false",
                "dateDebut": None,
                "dateFin": None,
                "mesuresCorrigees": "false",
                "accordClient": "true",
            },
            availability={"C1", "C2", "C3", "C4", "C5"},
            metadata={
                "measurement": {
                    "name": "active-power",
                    "direction": "consumption",
                    "quantity": "power",
                    "type": "electrical",
                    "unit": "W",
                    "aggregation": "mean",
                }
            },
        ),
        # Courbe de puissance active consommée corrigées
        "consumption/power/active/corrected": MeasurementApiSpec(
            params={
                "initiateurLogin": None,
                "pointId": None,
                "mesuresTypeCode": "COURBE",
                "grandeurPhysique": "PA",
                "soutirage": "true",
                "injection": "false",
                "dateDebut": None,
                "dateFin": None,
                "mesuresCorrigees": "true",
                "accordClient": "true",
            },
            availability={"C1", "C2", "C3", "C4"},
            metadata={
                "measurement": {
                    "name": "active-power",
                    "direction": "consumption",
                    "unit": "W",
                    "aggregation": "mean",
                    "corrected": "true",
                }
            },
        ),
        # Courbe de puissance active produite brute
        # P1-P4
        "production/power/active/raw": MeasurementApiSpec(
            params={
                "initiateurLogin": None,
                "pointId": None,
                "mesuresTypeCode": "COURBE",
                "grandeurPhysique": "PA",
                "soutirage": "false",
                "injection": "true",
                "dateDebut": None,
                "dateFin": None,
                "mesuresCorrigees": "false",
                "accordClient": "true",
            },
            availability={"P1", "P2", "P3", "P4"},
            metadata={
                "measurement": {
                    "name": "active-power",
                    "direction": "production",
                    "unit": "W",
                    "aggregation": "mean",
                }
            },
        ),
        # Courbe de puissance active produite corrigées
        # P1-P3
        "production/power/active/corrected": MeasurementApiSpec(
            params={
                "initiateurLogin": None,
                "pointId": None,
                "mesuresTypeCode": "COURBE",
                "grandeurPhysique": "PA",
                "soutirage": "false",
                "injection": "true",
                "dateDebut": None,
                "dateFin": None,
                "mesuresCorrigees": "true",
                "accordClient": "true",
            },
            availability={"P1", "P2", "P3"},
            metadata={
                "measurement": {
                    "name": "active-power",
                    "direction": "production",
                    "unit": "W",
                    "aggregation": "mean",
                    "corrected": "true",
                }
            },
        ),
        # Productions globales quotidiennes
        # C5
        "consumption/energy/active/daily": MeasurementApiSpec(
            params={
                "initiateurLogin": None,
                "pointId": None,
                "mesuresTypeCode": "ENERGIE",
                "grandeurPhysique": "EA",
                "soutirage": "true",
                "injection": "false",
                "dateDebut": None,
                "dateFin": None,
                "mesuresCorrigees": "false",
                "accordClient": "true",
            },
            availability={"P4"},
            metadata={
                "measurement": {
                    "name": "energy",
                    "direction": "production",
                    "unit": "Wh",
                    "aggregation": "sum",
                    "corrected": "false",
                    "sampling-interval": "P1D",
                }
            },
        ),
        # Productions globales quotidiennes
        # P4
        "production/energy/active/daily": MeasurementApiSpec(
            params={
                "initiateurLogin": None,
                "pointId": None,
                "mesuresTypeCode": "ENERGIE",
                "grandeurPhysique": "EA",
                "soutirage": "false",
                "injection": "true",
                "dateDebut": None,
                "dateFin": None,
                "mesuresCorrigees": "false",
                "accordClient": "true",
            },
            availability={"P4"},
            metadata={
                "measurement": {
                    "name": "energy",
                    "direction": "production",
                    "unit": "Wh",
                    "aggregation": "sum",
                    "corrected": "false",
                    "sampling-interval": "P1D",
                }
            },
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
            resp = self.client.service.consulterMesuresDetaillees(demande)

        class Quoalise(ElementBase):
            name = "quoalise"
            namespace = "urn:quoalise:0"

        quoalise_element = Quoalise()

        xmldata = ET.Element("data")
        quoalise_element.xml.append(xmldata)

        meta = ET.Element("meta")
        xmldata.append(meta)

        device = ET.Element("device", attrib={"type": "electricity-meter"})
        meta.append(device)
        device.append(
            ET.Element(
                "identifier",
                attrib={"authority": "enedis", "type": "prm", "value": usage_point_id},
            )
        )

        measurement_meta = ET.Element(
            "measurement", attrib=measurement.metadata["measurement"]
        )
        meta.append(measurement_meta)

        sensml = ET.Element("sensml", xmlns="urn:ietf:params:xml:ns:senml")
        xmldata.append(sensml)

        assert resp.grandeur[0].unite == measurement_meta.attrib["unit"]

        first = True
        if "sampling-interval" not in measurement_meta.attrib:
            measurement_meta.attrib["sampling-interval"] = resp.grandeur[0].mesure[0].p

        m = re.match(r"^PT(\d+)M$", measurement_meta.attrib["sampling-interval"])
        if m:
            # load curve is stamped by SGE API at the end of measurement periods
            # TODO, is this true for all segments? (see HDM)
            time_offset = int(m.group(1)) * 60
        elif measurement_meta.attrib["sampling-interval"] == "P1D":
            # daily data is stamped at the begining of the day
            time_offset = 0
        else:
            raise SgeError(
                "Unexpected time period: "
                + measurement_meta.attrib["sampling-interval"]
            )

        first = True
        bt = None
        for meas in resp.grandeur[0].mesure:
            v = str(meas.v)
            t = int(meas.d.astimezone(pytz.utc).timestamp()) - time_offset
            if "p" in dir(meas):
                assert meas.p == measurement_meta.attrib["sampling-interval"]
            if first:
                bt = t
                senml = ET.Element(
                    "senml",
                    bn=f"urn:dev:prm:{usage_point_id}_{name}",
                    bt=str(t),
                    t="0",
                    v=str(v),
                    bu=measurement_meta.attrib["unit"],
                )
                first = False
            else:
                assert bt is not None
                t = t - bt
                senml = ET.Element("senml", t=str(t), v=str(v))
            sensml.append(senml)

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
