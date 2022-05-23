#!/usr/bin/env python3

import datetime as dt
import suds

import lowatt_enedis
import lowatt_enedis.services

# TODO move to quoalise
import re
from slixmpp.xmlstream import ElementBase, ET
import pytz
import quoalise


class SgeError(Exception):
    def __init__(self, message, code=None):
        self.message = message
        self.code = code

    def __str__(self):
        if self.code is not None:
            return f"{self.code}: {self.message}"
        else:
            return self.message


class DetailedMeasurements:

    # TODO(cyril) ensure Python >= 3.7 to preserve dict order

    MEASUREMENTS = {
        # -------------------------
        # Courbes au pas enregistré
        #
        # 7 jours consécutifs maximum dans les 24 derniers mois par rapport
        # à la date du jour, limités à la dernière mise en service.
        #
        # Courbe de puissance active consommée brute
        "consumption/power/active/raw": {
            "params": {
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
            "availability": ["C1", "C2", "C3", "C4", "C5"],
            "metadata": {
                "measurement": {
                    "name": "active-power",
                    "direction": "consumption",
                    "quantity": "power",
                    "type": "electrical",
                    "unit": "W",
                    "aggregation": "mean",
                }
            },
        },
        # Courbe de puissance active consommée corrigées
        "consumption/power/active/corrected": {
            "params": {
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
            "availability": ["C1", "C2", "C3", "C4"],
            "metadata": {
                "measurement": {
                    "name": "active-power",
                    "direction": "consumption",
                    "unit": "W",
                    "aggregation": "mean",
                    "corrected": "true",
                }
            },
        },
        # Courbe de puissance active produite brute
        # P1-P4
        "production/power/active/raw": {
            "params": {
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
            "availability": ["P1", "P2", "P3", "P4"],
            "metadata": {
                "measurement": {
                    "name": "active-power",
                    "direction": "production",
                    "unit": "W",
                    "aggregation": "mean",
                }
            },
        },
        # Courbe de puissance active produite corrigées
        # P1-P3
        "production/power/active/corrected": {
            "params": {
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
            "availability": ["P1", "P2", "P3"],
            "metadata": {
                "measurement": {
                    "name": "active-power",
                    "direction": "production",
                    "unit": "W",
                    "aggregation": "mean",
                    "corrected": "true",
                }
            },
        },
        # Productions globales quotidiennes
        # C5
        "consumption/energy/active/daily": {
            "params": {
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
            "availability": ["P4"],
            "metadata": {
                "measurement": {
                    "name": "energy",
                    "direction": "production",
                    "unit": "Wh",
                    "aggregation": "sum",
                    "corrected": "false",
                    "sampling-interval": "P1D",
                }
            },
        },
        # Productions globales quotidiennes
        # P4
        "production/energy/active/daily": {
            "params": {
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
            "availability": ["P4"],
            "metadata": {
                "measurement": {
                    "name": "energy",
                    "direction": "production",
                    "unit": "Wh",
                    "aggregation": "sum",
                    "corrected": "false",
                    "sampling-interval": "P1D",
                }
            },
        },
    }

    def __init__(self, credentials):

        self.login = credentials["login"]
        certificate = credentials.abspath(credentials["certificate"])
        private_key = credentials.abspath(credentials["private-key"])
        homologation = credentials["environment"] != "production"

        self.client = lowatt_enedis.get_client(
            "ConsultationMesuresDetaillees-v2.0", certificate, private_key, homologation
        )

    # end date included (!= SGE API)
    def get_measurements(self, name, usage_point_id, start_date, end_date):

        measurement = self.MEASUREMENTS.get(name)

        if measurement is None:
            raise ValueError(f"{name} measurement is not known")

        if len(usage_point_id) != 14:
            raise ValueError(
                f"Usage point id {usage_point_id} must consist of 14 digits"
            )

        if not isinstance(start_date, dt.date):
            raise ValueError(
                (
                    f"Start date type should be datetime.date, "
                    f"not {type(start_date).__name__}"
                )
            )

        if not isinstance(end_date, dt.date):
            raise ValueError(
                f"End date type should be datetime.date, not {type(end_date).__name__}"
            )

        demande = measurement["params"].copy()
        demande.update(
            {
                "initiateurLogin": self.login,
                "pointId": usage_point_id,
                "dateDebut": quoalise.format_iso_date(start_date),
                "dateFin": quoalise.format_iso_date(end_date + dt.timedelta(days=1)),
            }
        )

        try:
            resp = self.client.service.consulterMesuresDetaillees(demande)
        except suds.WebFault as e:
            res = e.fault.detail.erreur.resultat
            raise SgeError(res.value, res._code)
        except Exception as e:
            # Suds client does not seem to handle HTTP errors very well,
            # it ends up raising Exception((503, 'Service Unavailable'))
            if len(e.args) == 1 and len(e.args[0]) == 2 and type(e.args[0][0]) == int:
                code = str(e.args[0][0])
                reason = str(e.args[0][1])
                raise SgeError(reason, code)
            else:
                raise e

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
            "measurement", attrib=measurement["metadata"]["measurement"]
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
            time_offset = int(m.group(1)) * 60
        elif measurement_meta.attrib["sampling-interval"] == "P1D":
            time_offset = 24 * 60 * 60
        else:
            raise SgeError(
                "Unexpected time period: "
                + measurement_meta.attrib["sampling-interval"]
            )

        first = True
        bt = None
        for measurement in resp.grandeur[0].mesure:
            v = str(measurement.v)
            t = int(measurement.d.astimezone(pytz.utc).timestamp()) - time_offset
            if "p" in dir(measurement):
                assert measurement.p == measurement_meta.attrib["sampling-interval"]
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
                t = t - bt
                senml = ET.Element("senml", t=str(t), v=str(v))
            sensml.append(senml)

        return quoalise_element
