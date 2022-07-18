import logging
import statistics
from typing import Dict, Iterable, Optional, List, Tuple
import xml.etree.ElementTree as ET
import datetime as dt
import pytz

from quoalise.data import Record


def _find_element(parent: ET.Element, tag: str) -> ET.Element:
    found = parent.find(tag)
    assert found is not None, f"Unable to find {tag}"
    return found


def _find_optional_element(parent: ET.Element, tag: str) -> Optional[ET.Element]:
    return parent.find(tag)


def _find_text(parent: ET.Element, tag: str) -> str:
    found = _find_element(parent, tag)
    assert found.text is not None, f"{tag} does not embed text"
    return found.text


RecordName = str
UsagePoint = str


class R171:
    def __init__(self, xml_doc: str) -> None:
        self.doc = ET.parse(xml_doc)

    def records(self) -> Iterable[Record]:

        computed_records: Dict[
            UsagePoint, Dict[dt.datetime, Dict[RecordName, Record]]
        ] = {}

        for series in self.doc.findall(".//serieMesuresDatees"):
            usage_point = _find_text(series, "prmId")
            direction = _find_text(series, "grandeurMetier")
            if direction == "CONS":
                direction = "consumption"
            elif direction == "PROD":
                # FIXME(cyril) check this
                direction = "production"
            else:
                raise RuntimeError(f"Unexpected direction {direction}")

            measurement_code = _find_text(series, "grandeurPhysique")
            unit = _find_text(series, "unite")
            temporal_class = _find_text(series, "codeClasseTemporelle").lower()
            temporal_class_owner = _find_text(series, "typeCalendrier")

            if temporal_class_owner == "D":
                temporal_class_owner = "distributor"
            else:
                raise RuntimeError("Is this supposed to happen?")

            base_name = f"urn:dev:prm:{usage_point}_{direction}"

            if measurement_code == "PMA":
                name = (
                    base_name
                    + f"/power/apparent/max/{temporal_class_owner}/{temporal_class}"
                )
            elif measurement_code == "EA":
                name = (
                    base_name
                    + f"/energy/active/index/{temporal_class_owner}/{temporal_class}"
                )
            else:
                # Data type not handled
                # EA Energie Active
                # PMA Puissance Maximale Atteinte
                # ERC Energie Réactive Capacitive ERI Energie Réactive Inductive
                # ER Energie Réactive
                # TF Temps de Fonctionnement
                # DD Durée de Dépassement
                # DE Dépassement Energétique
                # DQ Dépassement Quadratique
                continue

            for measurement in series.findall(".//mesureDatee"):
                time_str = _find_text(measurement, "dateFin")
                value = int(_find_text(measurement, "valeur"))
                # TODO(cyril) check that datetime is actually Paris time
                # No time zone is specified in R171
                # Most of the other files mention it, and its Paris time
                time = dt.datetime.fromisoformat(time_str)
                time = pytz.timezone("Europe/Paris").localize(time)

                yield Record(name, time, unit, value)

                # Autocomputed records

                if usage_point not in computed_records:
                    computed_records[usage_point] = {}

                base_name = f"urn:dev:prm:{usage_point}_consumption"

                if time not in computed_records[usage_point]:
                    computed_records[usage_point][time] = {
                        "power/apparent/max": Record(
                            f"{base_name}/power/apparent/max",
                            time,
                            None,
                            None,
                        ),
                        "energy/active/index": Record(
                            f"{base_name}/energy/active/index",
                            time,
                            None,
                            None,
                        ),
                    }

                if temporal_class_owner != "distributor":
                    # Use data from distributor counters
                    # (arbitrary, result should be the same with provider)
                    # TODO(cyril) check that distributor is always present in file
                    # (provider is not)
                    continue

                if direction != "consumption":
                    # Only compute records for consumption for now
                    continue

                if measurement_code == "PMA":
                    record = computed_records[usage_point][time]["power/apparent/max"]
                    if record.unit is None:
                        record.unit = unit
                    if record.value is None or record.value < value:
                        record.value = value

                elif measurement_code == "EA":
                    # TODO(cyril) check that sum is actually the main index
                    record = computed_records[usage_point][time]["energy/active/index"]
                    if record.unit is None:
                        record.unit = unit
                    if record.value is None:
                        record.value = value
                    else:
                        record.value += value

        for usage_point in computed_records:
            for time, records in computed_records[usage_point].items():
                for record in records.values():
                    assert record.value is not None, "Unable to compute record"
                    yield record


class R151:
    def __init__(self, xml_doc: str) -> None:
        self.doc = ET.parse(xml_doc)

    def records(self) -> Iterable[Record]:

        for prm in self.doc.findall(".//PRM"):
            usage_point = _find_text(prm, "Id_PRM")
            # FIXME(cyril) Nothing specified in this stream?
            # TODO(cyril) Check on a production usage point
            # (check in stream doc and commande collecte before)
            direction = "consumption"

            data = _find_element(prm, "Donnees_Releve")

            # TODO(cyril) specify what we want
            # like 2022-03-17
            time_str = _find_text(data, "Date_Releve")
            time = dt.datetime.fromisoformat(time_str)
            time = pytz.timezone("Europe/Paris").localize(time)

            base_name = f"urn:dev:prm:{usage_point}_{direction}"

            # FIXME(cyril) ensure sum is consumption only
            index_sum = 0

            temporal_class_owner = "distributor"
            for temporal_class in data.findall("./Classe_Temporelle_Distributeur"):

                value = int(_find_text(temporal_class, "Valeur"))
                temporal_class_id = _find_text(
                    temporal_class, "Id_Classe_Temporelle"
                ).lower()
                name = (
                    base_name
                    + f"/energy/active/index/{temporal_class_owner}/{temporal_class_id}"
                )

                record = Record(name, time, "Wh", value)

                yield record

                index_sum += value

            name = base_name + "/energy/active/index"
            record = Record(name, time, "Wh", index_sum)
            yield record

            temporal_class_owner = "provider"
            for temporal_class in data.findall("./Classe_Temporelle"):
                value = int(_find_text(temporal_class, "Valeur"))
                temporal_class_id = _find_text(
                    temporal_class, "Id_Classe_Temporelle"
                ).lower()
                unit = "Wh"
                name = (
                    base_name
                    + f"/energy/active/index/{temporal_class_owner}/{temporal_class_id}"
                )

                record = Record(name, time, unit, value)

                yield record

            pmax_element = _find_optional_element(data, "Puissance_Maximale")

            # Might not be present when data is not available
            if pmax_element is not None:
                value = int(_find_text(pmax_element, "Valeur"))
                name = base_name + "/power/apparent/max"
                unit = "VA"
                record = Record(name, time, unit, value)

                yield record


# Multiple files per zip archive
# Multiple PRMs per files
class R50:
    def __init__(self, xml_doc: str) -> None:
        self.doc = ET.parse(xml_doc)

    def records(self) -> Iterable[Record]:

        header = _find_element(self.doc, "En_Tete_Flux")
        period_minutes = int(_find_text(header, "Pas_Publication"))

        # Spec says its always 30 min but I thought this could be an hour too
        assert period_minutes == 30

        for prm in self.doc.findall("./PRM"):
            usage_point = _find_text(prm, "Id_PRM")
            # FIXME(cyril) Nothing specified in this stream?
            # TODO(cyril) Check on a production usage point
            # (check in stream doc and commande collecte before)
            direction = "consumption"

            name = f"urn:dev:prm:{usage_point}_{direction}/power/active/raw"

            # Just to check time period
            records: List[Tuple[dt.datetime, int]] = []

            for pdc in prm.findall("./Donnees_Releve/PDC"):
                datetime = _find_text(pdc, "H")
                value = int(_find_text(pdc, "V"))
                caution = int(_find_text(pdc, "IV"))

                datetime = dt.datetime.fromisoformat(datetime)

                # Data from file is stamped at the end of periods,
                # quoalise timestamp them at the begining
                datetime = datetime - dt.timedelta(minutes=period_minutes)

                records.append((datetime, value))

            datetimes = sorted(datetime for datetime, _ in records)
            periods = [j - i for i in datetimes[:-1] for j in datetimes[1:]]
            if periods:
                period_median = statistics.median(
                    [int(p.total_seconds() / 60) for p in periods]
                )
                assert period_median == period_minutes

            for datetime, value in records:
                record = Record(name, datetime, "W", value)

                yield record


class R4x:
    def __init__(self, xml_doc: str) -> None:
        self.doc = ET.parse(xml_doc)

    def records(self) -> Iterable[Record]:

        header = _find_element(self.doc, "Entete")
        nature = _find_text(header, "Nature_De_Courbe_Demandee")
        if nature == "Brute":
            nature = "raw"
        else:
            raise RuntimeError(f"Nature {nature} is not supported yet")

        body = _find_element(self.doc, "Corps")
        usage_point = _find_text(body, "Identifiant_PRM")

        for curve in body.findall("./Donnees_Courbe"):

            unit = _find_text(curve, "Unite_Mesure")

            period = int(_find_text(curve, "Granularite"))
            assert period == 10  # From spec

            direction = _find_text(curve, "Grandeur_Metier")
            if direction == "CONS":
                direction = "consumption"
            elif direction == "PROD":
                direction = "production"

            measurement = _find_text(curve, "Grandeur_Physique")
            if measurement == "EA":
                name = "power/active"
                assert unit == "kW"
                unit = "W"  # Value will be converted
            elif measurement == "ERC":
                # TODO(cyril) which standard unit do we want to use (currenly kVAr)
                name = "power/capacitive"
            elif measurement == "ERI":
                # TODO(cyril) which standard unit do we want to use (currenly kVAr)
                name = "power/inductive"
            elif measurement == "E":
                name = "voltage"
            else:
                raise RuntimeError(f"Unexpected Grandeur {name}")

            name = f"urn:dev:prm:{usage_point}_{direction}/{name}/{nature}"

            for point in curve.findall("./Donnees_Point_Mesure"):

                datetime = dt.datetime.fromisoformat(point.attrib["Horodatage"])
                value = int(point.attrib["Valeur_Point"])
                status = point.attrib["Statut_Point"]

                if status != "R":
                    # R : Réel
                    # H : Puissance reconstituée
                    # P : Puissance reconstituée et Coupure Secteur
                    # S : Coupure secteur
                    # T : Coupure Secteur courte
                    # F : Début coupure secteur
                    # G : Fin de coupure secteur
                    # E : Estimé
                    # C : Corrigé
                    # K : Calculé, point de courbe issu d’un calcul basé sur d’autres courbes de charges
                    # D : importé manuellement par le métier Enedis
                    logging.warn(f"status {status} is not handled yet")
                    continue

                if measurement == "EA":
                    # sge tiers proxy uses W
                    value = value * 1000

                record = Record(name, datetime, unit, value)

                yield record
