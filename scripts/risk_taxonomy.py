#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
risk_taxonomy.py

OSINT-alapú balkáni biztonsági előjelző rendszerhez
alap taxonómia, kulcsszavas osztályozás, súlyozás és szint-hozzárendelés.

Első verzió:
- egyszerű, determinisztikus
- külső ML nélkül működik
- könnyen bővíthető

Használat:
    from risk_taxonomy import classify_record, score_to_level
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple, Optional


# ---------------------------------------------------------------------
# Alap konstansok
# ---------------------------------------------------------------------

RISK_LEVELS = ["normal", "elevated", "tense", "critical"]

DIMENSIONS = [
    "political",
    "military",
    "policing",
    "migration",
    "social",
    "infrastructure",
]

COUNTRIES = [
    "Albania",
    "Bosnia and Herzegovina",
    "Bulgaria",
    "Croatia",
    "Greece",
    "Kosovo",
    "Montenegro",
    "North Macedonia",
    "Romania",
    "Serbia",
    "Slovenia",
]

# Kulcsfontosságú gócpontok, ahol azonos súlyosság mellett is nagyobb
# figyelmet érdemel egy incidens.
SENSITIVE_LOCATIONS = {
    "north kosovo": 1.25,
    "mitrovica": 1.20,
    "zvecan": 1.20,
    "leposavic": 1.20,
    "zubin potok": 1.20,
    "bosnia": 1.05,
    "republika srpska": 1.20,
    "brcko": 1.15,
    "sandzak": 1.10,
    "presevo": 1.10,
    "bujanovac": 1.10,
    "evros": 1.15,
    "aegean": 1.10,
    "unofficial migrant camp": 1.15,
    "border crossing": 1.10,
    "green border": 1.15,
}

# Forrástípusok becsült megbízhatósági súlya.
SOURCE_WEIGHTS = {
    "official": 1.20,
    "international_org": 1.15,
    "major_media": 1.00,
    "regional_media": 0.95,
    "local_media": 0.90,
    "aggregator": 0.75,
    "unknown": 0.80,
}

# Eseménytípus -> alap súlyosság
EVENT_SEVERITY = {
    "statement": 1,
    "diplomatic_tension": 2,
    "government_crisis": 3,
    "constitutional_dispute": 3,
    "election_tension": 2,
    "protest": 2,
    "violent_protest": 3,
    "ethnic_tension": 3,
    "blockade": 3,
    "strike_wave": 2,
    "police_operation": 2,
    "riot": 3,
    "shooting": 3,
    "explosion": 4,
    "terror_alert": 4,
    "organized_crime": 2,
    "border_incident": 3,
    "troop_movement": 4,
    "military_exercise": 2,
    "airspace_violation": 3,
    "armed_clash": 4,
    "migrant_pressure": 2,
    "smuggling_network": 2,
    "camp_tension": 2,
    "mass_detention": 2,
    "critical_infra_incident": 3,
    "cyber_incident": 2,
    "deescalation": 1,
    "cooperation": 1,
    "unknown": 1,
}

# A score -> szint első, egyszerű verziója
LEVEL_THRESHOLDS = {
    "normal": 0.0,
    "elevated": 2.5,
    "tense": 5.0,
    "critical": 8.0,
}


# ---------------------------------------------------------------------
# Kulcsszavas szabályrendszer
# ---------------------------------------------------------------------

KEYWORDS_BY_EVENT: Dict[str, List[str]] = {
    "armed_clash": [
        "armed clash", "exchange of fire", "gun battle", "firefight",
        "clash with police", "clash with security forces"
    ],
    "troop_movement": [
        "troop deployment", "troops deployed", "military deployment",
        "mobilization", "combat readiness", "reserve forces", "armored vehicles"
    ],
    "military_exercise": [
        "military exercise", "live-fire exercise", "drill", "joint exercise",
        "nato drill", "training operation"
    ],
    "airspace_violation": [
        "airspace violation", "drone incursion", "unauthorized drone",
        "air policing alert"
    ],
    "border_incident": [
        "border incident", "crossed the border", "frontier incident",
        "border police", "border tensions", "border standoff"
    ],
    "government_crisis": [
        "government crisis", "cabinet crisis", "confidence vote",
        "coalition collapse", "prime minister resigned", "government resigned"
    ],
    "constitutional_dispute": [
        "constitutional court", "constitutional dispute", "constitutional crisis",
        "institutional deadlock"
    ],
    "election_tension": [
        "election dispute", "election fraud", "contested vote",
        "boycott election", "electoral tensions"
    ],
    "diplomatic_tension": [
        "summoned ambassador", "diplomatic tension", "strongly condemned",
        "retaliatory measure", "bilateral tension"
    ],
    "protest": [
        "protest", "demonstration", "rally", "march", "sit-in"
    ],
    "violent_protest": [
        "violent protest", "protesters clashed", "stones were thrown",
        "tear gas", "water cannon", "riot police"
    ],
    "ethnic_tension": [
        "ethnic tension", "inter-ethnic", "communal tension",
        "nationalist slogans", "sectarian tension"
    ],
    "blockade": [
        "road blockade", "barricade", "border blockade", "blocked crossing",
        "transport blockade"
    ],
    "strike_wave": [
        "general strike", "strike wave", "nationwide strike", "work stoppage"
    ],
    "police_operation": [
        "police raid", "security operation", "anti-smuggling operation",
        "special police unit", "law enforcement action"
    ],
    "riot": [
        "riot", "rioting", "mob violence", "urban unrest"
    ],
    "shooting": [
        "shooting", "shots fired", "gunfire", "firearms incident"
    ],
    "explosion": [
        "explosion", "blast", "detonation", "improvised explosive device", "ied"
    ],
    "terror_alert": [
        "terror alert", "terror threat", "counterterrorism alert", "extremist plot"
    ],
    "organized_crime": [
        "organized crime", "criminal network", "drug trafficking",
        "weapons trafficking", "mafia", "cartel"
    ],
    "migrant_pressure": [
        "migrant pressure", "migrant surge", "illegal crossings",
        "irregular migration", "border crossing attempts", "migrant route"
    ],
    "smuggling_network": [
        "human smuggling", "smuggling ring", "trafficking network",
        "smugglers arrested"
    ],
    "camp_tension": [
        "migrant camp tension", "camp unrest", "reception center incident"
    ],
    "mass_detention": [
        "mass detention", "mass arrest", "detained dozens", "sweep operation"
    ],
    "critical_infra_incident": [
        "power outage", "grid disruption", "pipeline incident",
        "rail disruption", "port disruption", "airport disruption",
        "critical infrastructure"
    ],
    "cyber_incident": [
        "cyberattack", "ddos", "ransomware", "systems outage", "hacked"
    ],
    "deescalation": [
        "de-escalation", "talks resumed", "agreement reached", "ceasefire",
        "confidence-building", "joint statement"
    ],
    "cooperation": [
        "joint patrol", "cooperation agreement", "working group", "coordination meeting"
    ],
}

EVENT_TO_DIMENSIONS: Dict[str, List[str]] = {
    "statement": ["political"],
    "diplomatic_tension": ["political"],
    "government_crisis": ["political"],
    "constitutional_dispute": ["political"],
    "election_tension": ["political", "social"],
    "protest": ["social"],
    "violent_protest": ["social", "policing"],
    "ethnic_tension": ["social", "political"],
    "blockade": ["social", "policing"],
    "strike_wave": ["social"],
    "police_operation": ["policing"],
    "riot": ["policing", "social"],
    "shooting": ["policing"],
    "explosion": ["policing", "military"],
    "terror_alert": ["policing"],
    "organized_crime": ["policing"],
    "border_incident": ["military", "policing", "migration"],
    "troop_movement": ["military"],
    "military_exercise": ["military"],
    "airspace_violation": ["military"],
    "armed_clash": ["military", "policing"],
    "migrant_pressure": ["migration"],
    "smuggling_network": ["migration", "policing"],
    "camp_tension": ["migration", "social"],
    "mass_detention": ["migration", "policing"],
    "critical_infra_incident": ["infrastructure"],
    "cyber_incident": ["infrastructure", "political"],
    "deescalation": ["political", "social"],
    "cooperation": ["political", "policing"],
    "unknown": ["political"],
}


# ---------------------------------------------------------------------
# Adatosztály az osztályozott rekordhoz
# ---------------------------------------------------------------------

@dataclass
class ClassifiedRecord:
    title: str
    summary: str
    source_name: str
    source_type: str
    country: Optional[str]
    location_text: str
    event_type: str
    dimensions: List[str]
    severity: int
    confidence: float
    source_weight: float
    geo_weight: float
    base_score: float
    level_hint: str
    matched_keywords: List[str]

    def to_dict(self) -> Dict:
        return asdict(self)


# ---------------------------------------------------------------------
# Segédfüggvények
# ---------------------------------------------------------------------

def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    value = value.lower().strip()
    value = re.sub(r"\s+", " ", value)
    return value


def compile_text(title: Optional[str], summary: Optional[str], location_text: Optional[str]) -> str:
    parts = [
        normalize_text(title),
        normalize_text(summary),
        normalize_text(location_text),
    ]
    return " | ".join([p for p in parts if p])


def detect_country(text: str) -> Optional[str]:
    if not text:
        return None

    aliases = {
        "Albania": ["albania", "tirana"],
        "Bosnia and Herzegovina": ["bosnia and herzegovina", "bosnia", "sarajevo", "republika srpska", "banja luka"],
        "Bulgaria": ["bulgaria", "sofia"],
        "Croatia": ["croatia", "zagreb"],
        "Greece": ["greece", "athens", "evros", "aegean"],
        "Kosovo": ["kosovo", "pristina", "mitrovica", "zvecan", "zubin potok", "leposavic"],
        "Montenegro": ["montenegro", "podgorica", "bar"],
        "North Macedonia": ["north macedonia", "skopje", "macedonia"],
        "Romania": ["romania", "bucharest"],
        "Serbia": ["serbia", "belgrade", "presevo", "bujanovac", "sandzak"],
        "Slovenia": ["slovenia", "ljubljana"],
    }

    for country, words in aliases.items():
        for word in words:
            if word in text:
                return country
    return None


def detect_source_type(source_name: Optional[str]) -> str:
    source = normalize_text(source_name)

    official_markers = [
        "ministry", "government", "president", "prime minister",
        "police", "army", "mod", "moia", "interior ministry",
        "defence ministry", "border police"
    ]
    international_markers = [
        "nato", "eufor", "kfor", "eu", "frontex", "unhcr", "iom", "osce", "un "
    ]
    major_media_markers = [
        "reuters", "ap", "associated press", "afp", "bbc", "cnn", "euronews"
    ]
    regional_media_markers = [
        "balkan", "n1", "b92", "nova", "euractiv"
    ]
    aggregator_markers = [
        "gdelt", "google news", "newsnow", "aggregator", "rss"
    ]

    if any(m in source for m in official_markers):
        return "official"
    if any(m in source for m in international_markers):
        return "international_org"
    if any(m in source for m in major_media_markers):
        return "major_media"
    if any(m in source for m in regional_media_markers):
        return "regional_media"
    if any(m in source for m in aggregator_markers):
        return "aggregator"
    return "unknown"


def detect_geo_weight(text: str) -> float:
    weight = 1.0
    for location, multiplier in SENSITIVE_LOCATIONS.items():
        if location in text:
            weight = max(weight, multiplier)
    return weight


def match_event_type(text: str) -> Tuple[str, List[str]]:
    hits: List[Tuple[str, List[str]]] = []

    for event_type, keywords in KEYWORDS_BY_EVENT.items():
        matched = [kw for kw in keywords if kw in text]
        if matched:
            hits.append((event_type, matched))

    if not hits:
        return "unknown", []

    # A legtöbb találatot adó event_type nyer
    hits.sort(key=lambda item: len(item[1]), reverse=True)
    return hits[0][0], hits[0][1]


def event_dimensions(event_type: str) -> List[str]:
    return EVENT_TO_DIMENSIONS.get(event_type, ["political"])


def event_severity(event_type: str) -> int:
    return EVENT_SEVERITY.get(event_type, 1)


def estimate_confidence(
    event_type: str,
    matched_keywords: List[str],
    source_type: str,
    has_country: bool,
) -> float:
    confidence = 0.45

    if event_type != "unknown":
        confidence += 0.15

    confidence += min(len(matched_keywords) * 0.07, 0.21)

    if source_type in {"official", "international_org", "major_media"}:
        confidence += 0.10

    if has_country:
        confidence += 0.05

    return max(0.30, min(confidence, 0.95))


def compute_base_score(
    severity: int,
    source_weight: float,
    confidence: float,
    geo_weight: float,
) -> float:
    score = severity * source_weight * confidence * geo_weight
    return round(score, 2)


def score_to_level(score: float) -> str:
    if score >= LEVEL_THRESHOLDS["critical"]:
        return "critical"
    if score >= LEVEL_THRESHOLDS["tense"]:
        return "tense"
    if score >= LEVEL_THRESHOLDS["elevated"]:
        return "elevated"
    return "normal"


def classify_record(
    title: Optional[str],
    summary: Optional[str] = None,
    source_name: Optional[str] = None,
    country: Optional[str] = None,
    location_text: Optional[str] = None,
) -> ClassifiedRecord:
    """
    Egy hír/rekord elsődleges osztályozása.

    Paraméterek:
        title: cím
        summary: rövid leírás / snippet
        source_name: forrás neve
        country: ha upstream már tudja, átadható
        location_text: helyszín vagy földrajzi mező

    Visszatérés:
        ClassifiedRecord
    """
    text = compile_text(title, summary, location_text)
    inferred_country = country or detect_country(text)
    source_type = detect_source_type(source_name)
    source_weight = SOURCE_WEIGHTS.get(source_type, SOURCE_WEIGHTS["unknown"])
    geo_weight = detect_geo_weight(text)
    event_type, matched_keywords = match_event_type(text)
    dimensions = event_dimensions(event_type)
    severity = event_severity(event_type)
    confidence = estimate_confidence(
        event_type=event_type,
        matched_keywords=matched_keywords,
        source_type=source_type,
        has_country=bool(inferred_country),
    )
    base_score = compute_base_score(
        severity=severity,
        source_weight=source_weight,
        confidence=confidence,
        geo_weight=geo_weight,
    )
    level_hint = score_to_level(base_score)

    return ClassifiedRecord(
        title=title or "",
        summary=summary or "",
        source_name=source_name or "",
        source_type=source_type,
        country=inferred_country,
        location_text=location_text or "",
        event_type=event_type,
        dimensions=dimensions,
        severity=severity,
        confidence=confidence,
        source_weight=source_weight,
        geo_weight=geo_weight,
        base_score=base_score,
        level_hint=level_hint,
        matched_keywords=matched_keywords,
    )


def classify_many(records: List[Dict]) -> List[Dict]:
    """
    Egyszerű batch helper lista feldolgozására.

    Bemenet rekord példa:
    {
        "title": "...",
        "summary": "...",
        "source_name": "...",
        "country": "...",
        "location_text": "..."
    }
    """
    output: List[Dict] = []

    for item in records:
        classified = classify_record(
            title=item.get("title"),
            summary=item.get("summary"),
            source_name=item.get("source_name"),
            country=item.get("country"),
            location_text=item.get("location_text"),
        )
        row = dict(item)
        row.update({
            "classified": classified.to_dict()
        })
        output.append(row)

    return output


# ---------------------------------------------------------------------
# Kézi futtatási példa
# ---------------------------------------------------------------------

if __name__ == "__main__":
    sample_records = [
        {
            "title": "Police raid after shooting near North Kosovo border crossing",
            "summary": "Security forces launched an operation after shots were fired close to a sensitive crossing.",
            "source_name": "Regional Security News",
            "location_text": "North Kosovo border crossing",
        },
        {
            "title": "Government crisis deepens after coalition collapse in Sarajevo",
            "summary": "Officials confirmed renewed constitutional dispute and possible early election tensions.",
            "source_name": "Reuters",
            "location_text": "Sarajevo, Bosnia and Herzegovina",
        },
        {
            "title": "Joint patrol announced by border police and Frontex",
            "summary": "The cooperation aims to reduce irregular migration pressure.",
            "source_name": "Frontex",
            "location_text": "Serbia-Hungary border",
        },
    ]

    results = classify_many(sample_records)
    for item in results:
        print("=" * 80)
        print(item["title"])
        print(item["classified"])
