from typing import Dict, Iterable
import xml.etree.ElementTree as ET
import datetime as dt
import pytz

from quoalise.data import Record


def _find_element(parent: ET.Element, tag: str) -> ET.Element:
    found = parent.find(tag)
    assert found is not None, f"Unable to find {tag}"
    return found


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

            pmax_element = _find_element(data, "Puissance_Maximale")

            value = int(_find_text(pmax_element, "Valeur"))
            name = base_name + "/power/apparent/max"
            unit = "VA"
            record = Record(name, time, unit, value)

            yield record
