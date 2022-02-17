#!/usr/bin/env python3

import datetime as dt
import suds
import urllib

import lowatt_enedis
import lowatt_enedis.services

# TODO move to quoalise
import re
from slixmpp.xmlstream import ElementBase, ET
import pytz
import datetime as dt
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

        'consumption/active_power/raw': {
            'params': {
                'initiateurLogin': None,
                'pointId': None,
                'mesuresTypeCode': 'COURBE',
                'grandeurPhysique': 'PA',
                'soutirage': 'true',
                'injection': 'false',
                'dateDebut': None,
                'dateFin': None,
                'mesuresCorrigees': 'false',
                'accordClient': 'true'
            },
            'availability': ['C1', 'C2', 'C3', 'C4', 'C5'],
            'metadata': {
                'measurement': {
                    'name': "active-power",
                    'direction': 'consumption',
                    'quantity': 'power',
                    'type': 'electrical',
                    'unit': 'W',
                    'aggregation': 'mean'
                }
            }
        },

        # Courbe de puissance active consommée corrigées

        'consumption/active_power/corrected': {
            'params': {
                'initiateurLogin': None,
                'pointId': None,
                'mesuresTypeCode': 'COURBE',
                'grandeurPhysique': 'PA',
                'soutirage': 'true',
                'injection': 'false',
                'dateDebut': None,
                'dateFin': None,
                'mesuresCorrigees': 'true',
                'accordClient': 'true'
            },
            'availability': ['C1', 'C2', 'C3', 'C4'],
            'metadata': {
                'measurement': {
                    'name': "active-power",
                    'direction': 'consumption',
                    'unit': 'W',
                    'aggregation': 'mean',
                    'corrected': 'true'
                }
            }
        },

        # Courbe de puissance active produite brute
        # P1-P4

        'production/active_power/raw': {
            'params': {
                'initiateurLogin': None,
                'pointId': None,
                'mesuresTypeCode': 'COURBE',
                'grandeurPhysique': 'PA',
                'soutirage': 'false',
                'injection': 'true',
                'dateDebut': None,
                'dateFin': None,
                'mesuresCorrigees': 'false',
                'accordClient': 'true'
            },
            'availability': ['P1', 'P2', 'P3', 'P4'],
            'metadata': {
                'measurement': {
                    'name': "active-power",
                    'direction': 'production',
                    'unit': 'W',
                    'aggregation': 'mean',
                }
            }
        },
        # Courbe de puissance active produite corrigées
        # P1-P3

        'production/active_power/corrected': {
            'params': {
                'initiateurLogin': None,
                'pointId': None,
                'mesuresTypeCode': 'COURBE',
                'grandeurPhysique': 'PA',
                'soutirage': 'false',
                'injection': 'true',
                'dateDebut': None,
                'dateFin': None,
                'mesuresCorrigees': 'true',
                'accordClient': 'true'
            },
            'availability': ['P1', 'P2', 'P3'],
            'metadata': {
                'measurement': {
                    'name': "active-power",
                    'direction': 'production',
                    'unit': 'W',
                    'aggregation': 'mean',
                    'corrected': 'true'
                }
            }
        },
    }

    def __init__(self, credentials):

        self.login = credentials['login']
        certificate = credentials.abspath(credentials['certificate'])
        private_key = credentials.abspath(credentials['private-key'])
        homologation = credentials['environment'] != 'production'

        self.client = lowatt_enedis.get_client('ConsultationMesuresDetaillees-v2.0',
                                               certificate,
                                               private_key,
                                               homologation)

    # end date included (!= SGE API)
    def get_measurements(self, name, usage_point_id, start_date, end_date):

        measurement = self.MEASUREMENTS.get(name)

        if measurement is None:
            raise ValueError(f'{name} measurement is not known')

        if len(usage_point_id) != 14:
            raise ValueError(f'Usage point id {usage_point_id} must consist of 14 digits')

        if not isinstance(start_date, dt.date):
            raise ValueError(f'Start date type should be datetime.date, not {type(start_date).__name__}')

        if not isinstance(end_date, dt.date):
            raise ValueError(f'End date type should be datetime.date, not {type(end_date).__name__}')

        demande = measurement['params'].copy()
        demande.update({
            'initiateurLogin': self.login,
            'pointId': usage_point_id,
            'dateDebut': quoalise.format_iso_date(start_date),
            'dateFin': quoalise.format_iso_date(end_date + dt.timedelta(days=1))
        })

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
            name = 'quoalise'
            namespace = 'urn:quoalise:0'

        quoalise_element = Quoalise()

        xmldata = ET.Element('data')
        quoalise_element.xml.append(xmldata)

        meta = ET.Element('meta')
        xmldata.append(meta)

        device = ET.Element('device', attrib={'type': "electricity-meter"})
        meta.append(device)
        device.append(ET.Element('identifier', attrib={'authority': "enedis", 'type': "prm", 'value': usage_point_id}))

        measurement_meta = ET.Element('measurement', attrib=measurement['metadata']['measurement'])
        meta.append(measurement_meta)

        sensml = ET.Element('sensml', xmlns="urn:ietf:params:xml:ns:senml")
        xmldata.append(sensml)

        assert(resp.grandeur[0].unite == measurement_meta.attrib['unit'])

        first = True
        measurement_meta.attrib['sampling-interval'] = resp.grandeur[0].mesure[0].p

        m = re.match(r'^PT(\d+)M$', measurement_meta.attrib['sampling-interval'])
        if not m:
            raise SgeError(f"Unexpected time period: {period}")

        time_offset = int(m.group(1))*60

        first = True
        bt = None
        for measurement in resp.grandeur[0].mesure:
            v = str(measurement.v)
            t = int(measurement.d.astimezone(pytz.utc).timestamp()) - time_offset
            assert(measurement.p == measurement_meta.attrib['sampling-interval'])
            if first:
                bt = t
                senml = ET.Element('senml',
                                   bn=f"urn:dev:prm:{usage_point_id}_{name}",
                                   bt=str(t), t='0', v=str(v), bu=measurement_meta.attrib['unit'])
                first = False
            else:
                t = t - bt
                senml = ET.Element('senml', t=str(t), v=str(v))
            sensml.append(senml)

        return quoalise_element

if __name__ == '__main__':

    import argparse
    import sys
    from config import Config

    # TODO move to quoalise
    from slixmpp.xmlstream import ET, tostring

    parser = argparse.ArgumentParser()
    parser.add_argument('sge_credentials', help="Credentials to access SGE Tiers (typically private/*.conf.json)")
    parser.add_argument('measurement', help="Measurement name (like consumption/active_power/raw)")
    parser.add_argument('prm', help="Usage point id")
    parser.add_argument('start_date', help="Start date, included (YYYY-MM-DD)")
    parser.add_argument('end_date', help="End date, excluded (YYYY-MM-DD)")
    args = parser.parse_args()

    sge_credentials = Config(args.sge_credentials)

    api = DetailedMeasurements(sge_credentials)
    quoalise = api.get_mesurements(args.measurement, args.prm, args.start_date, args.end_date)

    print(tostring(quoalise.xml))

    # TODO to add to unit tests
    # When asking for data from 2021-02-01, getting response :

    # periode =
    #   (PeriodeType){
    #      dateDebut = 2021-02-01
    #      dateFin = 2021-02-05
    #   }
    # grandeur[] =
    #   (GrandeurType){
    #      grandeurMetier = "CONS"
    #      grandeurPhysique = "PA"
    #      unite = "W"
    #      mesure[] =
    #         (MesureType){
    #            v = 547
    #            d = 2021-02-01 01:00:00+01:00
    #            p = "PT60M"
    #            n = "B"
    #         },

    # First timestamp seems to be at the end of the first period
    # Quoalise measurement should be timestamped 1612134000 in senml

    assert(1612134000 == dt.datetime.strptime('2021-02-01 00:00:00+0100', '%Y-%m-%d %H:%M:%S%z').timestamp())

    # first data stamp should be 1612137600

    # bt should be equal to 2021-02-01 UTC
