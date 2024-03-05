import logging
import statistics
from typing import Dict, Iterable, Optional, List, Tuple, Union, TextIO
import xml.etree.ElementTree as ET
import datetime as dt
import zoneinfo
import re

from quoalise.data import Record
from sgeproxy.metadata import Metadata, SamplingInterval
from sgeproxy.metadata_enedis import (
    MetadataEnedisConsumptionPowerActiveRaw,
    MetadataEnedisConsumptionPowerApparentMax,
    MetadataEnedisConsumptionPowerActiveMax,
    MetadataEnedisConsumptionEnergyActiveIndex,
    MetadataEnedisConsumptionPowerCapacitiveRaw,
    MetadataEnedisConsumptionPowerInductiveRaw,
    MetadataEnedisConsumptionVoltageRaw,
    LOAD_CURVE_SAMPLING_INTERVALS,
)


def _find_element(parent: Union[ET.ElementTree, ET.Element], tag: str) -> ET.Element:
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

    def records(self) -> Iterable[Tuple[Metadata, Record]]:

        computed_records: Dict[
            UsagePoint, Dict[dt.datetime, Dict[RecordName, Tuple[Metadata, Record]]]
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
                temporal_class_owner = "provider"

            base_name = f"urn:dev:prm:{usage_point}_{direction}"
            ea_meta = MetadataEnedisConsumptionEnergyActiveIndex(usage_point)
            pmax_meta = MetadataEnedisConsumptionPowerApparentMax(usage_point)
            pmax_active_meta = MetadataEnedisConsumptionPowerActiveMax(usage_point)

            if measurement_code == "PMA":
                if unit == "VA":
                    name = (
                        base_name
                        + f"/power/apparent/max/{temporal_class_owner}/{temporal_class}"
                    )
                    meta: Metadata = pmax_meta
                elif unit == "W":
                    name = (
                        base_name
                        + f"/power/active/max/{temporal_class_owner}/{temporal_class}"
                    )
                    meta = pmax_active_meta
                else:
                    raise ValueError("Unexpected PMA unit")
            elif measurement_code == "EA":
                name = (
                    base_name
                    + f"/energy/active/index/{temporal_class_owner}/{temporal_class}"
                )
                meta = ea_meta
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

            assert (
                unit == meta.measurement.unit.value
            ), f"unit {unit} != {meta.measurement.unit.value} expected"

            for measurement in series.findall(".//mesureDatee"):
                # TODO(cyril) PMAX is relevant over a period of time, should be
                # stamped at the begining.
                time_str = _find_text(measurement, "dateFin")
                value = int(_find_text(measurement, "valeur"))
                # TODO(cyril) check that datetime is actually Paris time
                # No time zone is specified in R171
                # Most of the other files mention it, and its Paris time
                time = dt.datetime.fromisoformat(time_str)
                time = time.replace(tzinfo=zoneinfo.ZoneInfo("Europe/Paris"))

                yield meta, Record(name, time, unit, value)

                # Autocomputed records

                if usage_point not in computed_records:
                    computed_records[usage_point] = {}

                base_name = f"urn:dev:prm:{usage_point}_consumption"

                if time not in computed_records[usage_point]:
                    computed_records[usage_point][time] = {
                        "power/apparent/max": (
                            pmax_meta,
                            Record(
                                f"{base_name}/power/apparent/max",
                                time,
                                None,
                                None,
                            ),
                        ),
                        "power/active/max": (
                            pmax_active_meta,
                            Record(
                                f"{base_name}/power/active/max",
                                time,
                                None,
                                None,
                            ),
                        ),
                        "energy/active/index": (
                            ea_meta,
                            Record(
                                f"{base_name}/energy/active/index",
                                time,
                                None,
                                None,
                            ),
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

                if meta == pmax_meta:
                    meta, record = computed_records[usage_point][time][
                        "power/apparent/max"
                    ]
                    if record.unit is None:
                        record.unit = unit
                    assert record.unit == meta.measurement.unit.value
                    if record.value is None or record.value < value:
                        record.value = value

                elif meta == pmax_active_meta:
                    meta, record = computed_records[usage_point][time][
                        "power/active/max"
                    ]
                    if record.unit is None:
                        record.unit = unit
                    assert record.unit == meta.measurement.unit.value
                    if record.value is None or record.value < value:
                        record.value = value

                elif measurement_code == "EA":
                    # TODO(cyril) check that sum is actually the main index
                    meta, record = computed_records[usage_point][time][
                        "energy/active/index"
                    ]
                    if record.unit is None:
                        record.unit = unit
                    assert record.unit == meta.measurement.unit.value
                    if record.value is None:
                        record.value = value
                    else:
                        record.value += value

        for usage_point in computed_records:
            for time, records in computed_records[usage_point].items():
                for meta, record in records.values():
                    if record.value is not None:
                        yield meta, record


class R151:
    def __init__(self, xml_doc: str) -> None:
        self.doc = ET.parse(xml_doc)

    def records(self) -> Iterable[Tuple[Metadata, Record]]:

        for prm in self.doc.findall(".//PRM"):
            usage_point = _find_text(prm, "Id_PRM")
            # FIXME(cyril) Nothing specified in this stream?
            # TODO(cyril) Check on a production usage point
            # (check in stream doc and commande collecte before)
            direction = "consumption"

            ea_meta = MetadataEnedisConsumptionEnergyActiveIndex(usage_point)
            pmax_meta = MetadataEnedisConsumptionPowerApparentMax(usage_point)

            data = _find_element(prm, "Donnees_Releve")

            # TODO(cyril) specify what we want
            # like 2022-03-17
            time_str = _find_text(data, "Date_Releve")
            time = dt.datetime.fromisoformat(time_str)
            time = time.replace(tzinfo=zoneinfo.ZoneInfo("Europe/Paris"))

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

                yield ea_meta, record

                index_sum += value

            name = base_name + "/energy/active/index"
            record = Record(name, time, "Wh", index_sum)
            yield ea_meta, record

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

                yield ea_meta, record

            pmax_element = _find_optional_element(data, "Puissance_Maximale")

            # Might not be present when data is not available
            if pmax_element is not None:
                value = int(_find_text(pmax_element, "Valeur"))
                name = base_name + "/power/apparent/max"
                unit = "VA"
                # TODO(cyril) valid over a period of time,
                # should be stamped at the begining
                record = Record(name, time, unit, value)

                yield pmax_meta, record


# Multiple files per zip archive
# Multiple PRMs per files
class R50:
    def __init__(self, xml_doc: str) -> None:
        self.doc = ET.parse(xml_doc)

    def records(self) -> Iterable[Tuple[Metadata, Record]]:

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

            meta = MetadataEnedisConsumptionPowerActiveRaw(
                usage_point, SamplingInterval("PT30M")
            )

            name = f"urn:dev:prm:{usage_point}_{direction}/power/active/raw"

            # Just to check time period
            records: List[Tuple[dt.datetime, int]] = []

            for pdc in prm.findall("./Donnees_Releve/PDC"):
                datetime_str = _find_text(pdc, "H")
                # Sometime date is prensent but not value
                value_element = pdc.find("V")
                value_text = value_element.text if value_element is not None else None
                if value_text is None:
                    logging.warning(f"missing values for {usage_point}")
                    continue
                value = int(value_text)

                caution = int(_find_text(pdc, "IV"))
                if caution:
                    logging.warning(f"caution {caution} is not handled yet")

                datetime = dt.datetime.fromisoformat(datetime_str)

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

                yield meta, record


class R4x:
    def __init__(self, xml_doc: str) -> None:
        self.doc = ET.parse(xml_doc)

    def records(self) -> Iterable[Tuple[Metadata, Record]]:

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

            assert period in LOAD_CURVE_SAMPLING_INTERVALS.keys()
            sampling_interval = LOAD_CURVE_SAMPLING_INTERVALS[period]

            try:
                direction = _find_text(curve, "Grandeur_Metier")
            except AssertionError:
                # TODO I guess it is a bug, remove when no longer needed
                logging.warning("Grandeur_Metier missing, supposing CONS")
                direction = "CONS"
            if direction == "CONS":
                direction = "consumption"
            elif direction == "PROD":
                direction = "production"

            measurement = _find_text(curve, "Grandeur_Physique")
            if measurement == "EA":
                name = "power/active"
                assert unit == "kW"
                unit = "W"  # Value will be converted
                meta: Metadata = MetadataEnedisConsumptionPowerActiveRaw(
                    usage_point, sampling_interval
                )
            elif measurement == "ERC":
                name = "power/capacitive"
                # TODO seems to be kVAr
                # assert unit == "kWr"
                unit = "Wr"  # Value will be converted
                meta = MetadataEnedisConsumptionPowerCapacitiveRaw(
                    usage_point, sampling_interval
                )
            elif measurement == "ERI":
                # TODO seems to be kVAr
                # assert unit == "kWr"
                unit = "Wr"  # Value will be converted
                name = "power/inductive"
                meta = MetadataEnedisConsumptionPowerInductiveRaw(
                    usage_point, sampling_interval
                )
            elif measurement == "E":
                name = "voltage"
                meta = MetadataEnedisConsumptionVoltageRaw(
                    usage_point, sampling_interval
                )
            else:
                raise RuntimeError(f"Unexpected Grandeur {name}")

            name = f"urn:dev:prm:{usage_point}_{direction}/{name}/{nature}"

            for point in curve.findall("./Donnees_Point_Mesure"):

                datetime = dt.datetime.fromisoformat(point.attrib["Horodatage"])
                if "Valeur_Point" not in point.attrib:
                    logging.warning(f"missing values for {usage_point}")
                    continue
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
                    # K : Calculé, point de courbe issu d’un calcul basé sur
                    #     d’autres courbes de charges
                    # D : importé manuellement par le métier Enedis
                    logging.warning(f"status {status} is not handled yet")
                    continue

                if measurement in ["EA", "ERC", "ERI"]:
                    # sge tiers proxy uses W, not kW
                    value = value * 1000

                record = Record(name, datetime, unit, value)

                yield meta, record


class Hdm:
    @staticmethod
    def open(filename: str) -> TextIO:
        # Files starts with \ufeff
        return open(filename, mode="r", encoding="utf-8-sig")

    def __init__(self, csv_file: TextIO) -> None:
        self.csv_file = csv_file
        csv_meta_header = next(self.csv_file, "").strip().split(";")
        csv_meta_values = next(self.csv_file, "").strip().split(";")
        if len(csv_meta_header) == len(csv_meta_values):
            self.csv_meta = dict(zip(csv_meta_header, csv_meta_values))
        else:
            self.csv_meta = {}
            logging.warning("Unexpected meta")

    def usage_point(self):
        if self.csv_meta:
            return self.csv_meta["Identifiant PRM"]
        else:
            return None

    def records(self, is_c5) -> Iterable[Tuple[Metadata, Record]]:
        if not self.csv_meta:
            return
        if self.csv_meta["Type de donnees"] == "Courbe de charge":
            fn = self.cdc_records
        elif self.csv_meta["Type de donnees"] == "Index":
            fn = self.idx_records
        else:
            raise ValueError(f"Unexpected data type {self.csv_meta['Type de donnees']}")
        for meta, record in fn(is_c5):
            yield meta, record

    def cdc_records(self, is_c5) -> Iterable[Tuple[Metadata, Record]]:

        assert self.csv_meta["Type de donnees"] == "Courbe de charge"
        assert self.csv_meta["Grandeur physique"] == "Energie active"
        name = "power/active"  # TODO repeated from R4x, refactor

        assert self.csv_meta["Grandeur metier"] == "Consommation"
        direction = "consumption"

        assert self.csv_meta["Etape metier"] == "Comptage Brut"
        nature = "raw"

        # Unit does not seems to be specified sometimes, assume W
        if self.csv_meta["Unite"] == "W" or self.csv_meta["Unite"] == "":
            unit = "W"
        else:
            raise ValueError(f"Unexpected stream unit {self.csv_meta['Unite']}")

        # Sampling interval it not specified because it can change within a file

        usage_point = self.csv_meta["Identifiant PRM"]

        name = f"urn:dev:prm:{usage_point}_{direction}/{name}/{nature}"

        meta: Optional[Metadata] = None

        csv_values_header = next(self.csv_file).strip().split(";")
        assert csv_values_header == ["Horodate", "Valeur"]

        prev_time = None
        first_row = None
        rows: List[Tuple[dt.datetime, dt.timedelta, Optional[int]]] = []

        for row in self.csv_file:
            values = row.strip().split(";")
            assert len(values) == 2
            if values[0] == "":
                continue
            datetime = dt.datetime.fromisoformat(values[0])
            if values[1] != "":
                value = int(values[1])
            else:
                value = None

            if first_row is None:
                first_row = (datetime, None, value)
                prev_time = datetime
                continue
            else:
                diff = datetime - prev_time
                prev_time = datetime

            rows.append((datetime, diff, value))

        # Assume sampling first sampling rate is the same than the following
        if len(rows) < 1 or first_row is None:
            logging.warning("Not enough rows to infer sampling, skip")
            return
        rows.insert(0, (first_row[0], rows[0][1], first_row[2]))

        # C5 segment values are stamped at the end of the period,
        # C4 at the beginning,
        # quoalise at the beginning.

        if is_c5:
            rows = [(t - d, d, v) for t, d, v in rows]

        for datetime, diff, value in rows:

            if value is None:
                continue

            diff_minutes = round(diff.total_seconds() / 60)
            sampling_interval = LOAD_CURVE_SAMPLING_INTERVALS.get(diff_minutes)
            if sampling_interval is None:
                logging.warning(
                    f"Unexpected sampling interval {diff_minutes}, skip value"
                )
                continue
            meta = MetadataEnedisConsumptionPowerActiveRaw(
                usage_point, sampling_interval
            )
            record = Record(name, datetime, unit, value)
            yield meta, record

    def idx_records(self, is_c5) -> Iterable[Tuple[Metadata, Record]]:

        assert self.csv_meta["Type de donnees"] == "Index"
        assert self.csv_meta["Grandeur physique"] == "Energie active"

        ea_meta = MetadataEnedisConsumptionEnergyActiveIndex(self.usage_point())
        direction = "consumption"
        base_name = f"urn:dev:prm:{self.usage_point()}_{direction}"

        index_header = next(self.csv_file).strip().split(";")
        assert index_header == [
            "Horodate",
            "Type de releve",
            "EAS F1",
            "EAS F2",
            "EAS F3",
            "EAS F4",
            "EAS F5",
            "EAS F6",
            "EAS F7",
            "EAS F8",
            "EAS F9",
            "EAS F10",
            "EAS D1",
            "EAS D2",
            "EAS D3",
            "EAS D4",
            # Index totalisateur
            #
            # L’index totalisateur correspond à la somme de tous les
            # index (Fournisseur ou Distributeur) du compteur.
            # Cela signifie qu’il peut prendre en compte des index qui
            # ne sont pas transmis car non utilisés dans la grille
            # programmée à la date du relevé.
            # For instance with the following data, happening when switching
            # contract. Total remains the same, some sub counters no longer
            # appear in the data, but they are still used to compute the sum.
            #
            # 2020-10-13T00:00:00+02:00;Arrêté quotidien;42595832;39139315;1763620;7177480;634973;4148978;;;;;95460198;;;;95460198 # noqa: E501
            # 2020-10-14T00:00:00+02:00;Arrêté quotidien;42782598;39139315;1763620;7177480;634973;4148978;;;;;95646964;;;;95646964 # noqa: E501
            # 2020-10-15T00:00:00+02:00;Arrêté quotidien;42968066;;;;;;;;;;95832432;;;;95832432 # noqa: E501
            # 2020-10-16T00:00:00+02:00;Arrêté quotidien;43169396;;;;;;;;;;96033762;;;;96033762 # noqa: E501
            "EAS T",
        ]
        index_values = []
        got_values = False
        for row in self.csv_file:
            values = row.strip().split(";")
            if len(values) != len(index_header):
                break
            assert values[1] == "Arrêté quotidien"

            time = dt.datetime.fromisoformat(values[0])
            f_counters_str = values[2 : 2 + 10]
            d_counters_str = values[2 + 10 : 2 + 10 + 4]
            total_str = values[2 + 10 + 4]

            f_counters = [int(v) if v != "" else None for v in f_counters_str]
            d_counters = [int(v) if v != "" else None for v in d_counters_str]
            total = int(total_str) if total_str != "" else None

            if not got_values and total is not None:
                got_values = True

            index_values.append(
                (
                    time,
                    f_counters,
                    d_counters,
                    total,
                )
            )

        if got_values:
            # Calendar information does not seem to be included when
            # no index values are present, to be confirmed.
            calendar_header = row.strip().split(";")
            assert calendar_header == [
                "Periode",
                "Identifiant calendrier fournisseur",
                "Libelle calendrier fournisseur",
                "Identifiant classe temporelle 1",
                "Libelle classe temporelle 1",
                "Cadran classe temporelle 1",
                "Identifiant classe temporelle 2",
                "Libelle classe temporelle 2",
                "Cadran classe temporelle 2",
                "Identifiant classe temporelle 3",
                "Libelle classe temporelle 3",
                "Cadran classe temporelle 3",
                "Identifiant classe temporelle 4",
                "Libelle classe temporelle 4",
                "Cadran classe temporelle 4",
                "Identifiant classe temporelle 5",
                "Libelle classe temporelle 5",
                "Cadran classe temporelle 5",
                "Identifiant classe temporelle 6",
                "Libelle classe temporelle 6",
                "Cadran classe temporelle 6",
                "Identifiant classe temporelle 7",
                "Libelle classe temporelle 7",
                "Cadran classe temporelle 7",
                "Identifiant classe temporelle 8",
                "Libelle classe temporelle 8",
                "Cadran classe temporelle 8",
                "Identifiant classe temporelle 9",
                "Libelle classe temporelle 9",
                "Cadran classe temporelle 9",
                "Identifiant classe temporelle 10",
                "Libelle classe temporelle 10",
                "Cadran classe temporelle 10",
                "Identifiant calendrier distributeur",
                "Libelle calendrier distributeur",
                "Identifiant classe temporelle distributeur 1",
                "Libelle classe temporelle distributeur 1",
                "Cadran classe temporelle distributeur 1",
                "Identifiant classe temporelle distributeur 2",
                "Libelle classe temporelle distributeur 2",
                "Cadran classe temporelle distributeur 2",
                "Identifiant classe temporelle distributeur 3",
                "Libelle classe temporelle distributeur 3",
                "Cadran classe temporelle distributeur 3",
                "Identifiant classe temporelle distributeur 4",
                "Libelle classe temporelle distributeur 4",
                "Cadran classe temporelle distributeur 4",
            ]
            calendar_values = []
            for row in self.csv_file:
                values = row.strip().split(";")
                if len(values) != len(calendar_header):
                    # Most likely pmax meta header
                    break

                # Du 2020-03-31T00:00:00+02:00 au 2020-11-18T23:00:00+01:00
                m = re.match(r"^Du (.{25}) au (.{25})$", values[0])
                if m:
                    period_from = dt.datetime.fromisoformat(m.group(1))
                    period_to = dt.datetime.fromisoformat(m.group(2))
                else:
                    # Du 2020-11-18T23:00:00+01:00 au
                    m = re.match(r"^Du (.{25}) au$", values[0])
                    if m:
                        period_from = dt.datetime.fromisoformat(m.group(1))
                        period_to = dt.datetime.now().astimezone()
                    else:
                        raise ValueError(f"Unexpected calendar period {values[0]}")

                provider_ids = []
                for i in range(10):
                    provider_ids.append(values[3 + 3 * i].lower())

                distributor_ids = []
                for i in range(4):
                    distributor_ids.append(values[3 + 3 * 10 + 2 + 3 * i].lower())

                calendar_values.append(
                    (
                        period_from,
                        period_to,
                        provider_ids,
                        distributor_ids,
                    )
                )

            # Assert end of each period is the begining of the following
            for i in range(len(calendar_values) - 1):
                assert calendar_values[i][1] == calendar_values[i + 1][0]

            for cal_from, cal_to, provider_ids, distributor_ids in calendar_values:

                p_total_prev = None
                d_total_prev = None
                total_prev = None

                while len(index_values) > 0:
                    time, p_counters, d_counters, total = index_values[0]
                    assert time >= cal_from
                    if time >= cal_to:
                        break

                    # Sometimes index values can be found while the corresponding
                    # counter is not supposed to be used. Do not take those into
                    # account because it makes the diff check fail.

                    # For instance:
                    # 2022-02-27T23:00:00+01:00;Arrêté quotidien;10341184;29044825;;;;;;;;;40471015;;;;40471015 # noqa: E501
                    # 2022-02-28T23:00:00+01:00;Arrêté quotidien;10359381;29104538;191045;735201;36448;122312;0;0;0;0;40548925;0;0;0;40548925 # noqa: E501
                    # 2022-03-01T23:00:00+01:00;Arrêté quotidien;10374734;29155833;;;;;;;;;40615573;;;;40615573  # noqa: E501
                    for i in range(10):
                        if provider_ids[i] == "":
                            p_counters[i] = None

                    for i in range(4):
                        if distributor_ids[i] == "":
                            d_counters[i] = None

                    p_counters_ints = [v for v in p_counters if v is not None]
                    d_counters_ints = [v for v in d_counters if v is not None]

                    p_total = sum(p_counters_ints) if p_counters_ints else None
                    d_total = sum(d_counters_ints) if d_counters_ints else None

                    if total is not None and total_prev is not None:
                        diff = total - total_prev
                    else:
                        diff = None

                    if d_total is not None and d_total_prev is not None:
                        d_diff = d_total - d_total_prev
                        if d_diff != diff:
                            logging.warning("Unexpected distributor index")

                    if p_total is not None and p_total_prev is not None:
                        p_diff = p_total - p_total_prev
                        if p_diff != diff:
                            logging.warning("Unexpected provider index")

                    p_total_prev = p_total
                    d_total_prev = d_total
                    total_prev = total

                    index_values.pop(0)
                    if d_total is None:
                        if p_total is not None:
                            logging.warning("Idx for provider only")
                        continue

                    # Everything is ok, publish values, use distributor sum as total,
                    # like for daily streams, might change in the future.

                    temporal_class_owner = "provider"
                    for i in range(10):
                        temporal_class = provider_ids[i]
                        if temporal_class == "":
                            continue
                        name = (
                            base_name
                            + "/energy/active/index/"
                            + temporal_class_owner
                            + "/"
                            + temporal_class
                        )
                        record = Record(name, time, "Wh", p_counters[i])
                        yield ea_meta, record

                    temporal_class_owner = "distributor"
                    for i in range(4):
                        temporal_class = distributor_ids[i]
                        if temporal_class == "":
                            continue
                        name = (
                            base_name
                            + "/energy/active/index/"
                            + temporal_class_owner
                            + "/"
                            + temporal_class
                        )
                        record = Record(name, time, "Wh", d_counters[i])
                        yield ea_meta, record

                    name = base_name + "/energy/active/index"
                    record = Record(name, time, "Wh", d_total)
                    yield ea_meta, record

            assert len(index_values) == 0

        pmax_meta_header = row.strip().split(";")
        pmax_meta_values = next(self.csv_file).strip().split(";")

        assert len(pmax_meta_header) == len(pmax_meta_values)
        pmax_meta = dict(zip(pmax_meta_header, pmax_meta_values))

        assert pmax_meta["Identifiant PRM"] == self.usage_point()
        assert pmax_meta["Type de donnees"] == "Puissance maximale quotidienne"
        assert pmax_meta["Grandeur physique"] == "Puissance maximale atteinte"
        assert pmax_meta["Grandeur metier"] == "Consommation"
        # Seems to be empty sometimes
        # assert pmax_meta['Etape metier'] == 'Comptage Brut'
        # Seems to be empty sometimes
        # Docs tell this is an error, should be VA
        # assert pmax_meta['Unite'] == 'W'

        meta = MetadataEnedisConsumptionPowerApparentMax(self.usage_point())

        pmax_header = next(self.csv_file).strip().split(";")
        assert pmax_header == ["Horodate", "Valeur"]

        for row in self.csv_file:
            values = row.strip().split(";")
            assert len(values) == len(pmax_header)
            # Some row have no values specified
            if values[1] == "":
                continue
            time = dt.datetime.fromisoformat(values[0])
            value = int(values[1])
            name = base_name + "/power/apparent/max"
            unit = "VA"
            # TODO(cyril) valid over a period of time,
            # should be stamped at the begining
            record = Record(name, time, unit, value)
            yield meta, record
