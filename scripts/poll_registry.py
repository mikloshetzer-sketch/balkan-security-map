#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
poll_registry.py

Első építőkocka a balkáni pártpreferencia-követéshez.

Cél:
- a repo országkörének automatikus felismerése a docs/data/meta.json alapján
- országonkénti forrás-regiszter felépítése
- annak ellenőrzése, hogy van-e legalább 3 használható forrás egy országra
- egy későbbi fetch/aggregate script stabil bemeneti szerkezetének biztosítása

FONTOS:
Ez a fájl MÉG NEM scrape-el.
Ez a konfigurációs és ellenőrző réteg, amire a következő fájl épül majd.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
META_PATH = REPO_ROOT / "docs" / "data" / "meta.json"


# ---------------------------------------------------------------------
# Adatmodellek
# ---------------------------------------------------------------------

@dataclass
class PollSource:
    source_id: str
    source_name: str
    category: str  # local | aggregator | regional | bootstrap
    country: str
    homepage: str
    supports_party_polling: bool
    supports_trend_context: bool
    parser_kind: str  # html_table | json_api | manual_csv | custom_html | disabled
    polling_frequency_hint_days: Optional[int]
    priority: int
    active: bool
    confidence: str  # high | medium | low
    notes: str = ""
    tags: List[str] = field(default_factory=list)


@dataclass
class CountryPollConfig:
    country: str
    enabled: bool
    region_group: str
    election_type: str
    source_minimum: int
    parties_namespace: str
    sources: List[PollSource] = field(default_factory=list)

    def active_party_sources(self) -> List[PollSource]:
        return [
            src
            for src in self.sources
            if src.active and src.supports_party_polling
        ]

    def active_context_sources(self) -> List[PollSource]:
        return [
            src
            for src in self.sources
            if src.active and src.supports_trend_context
        ]

    def ready_for_aggregation(self) -> bool:
        return len(self.active_party_sources()) >= self.source_minimum

    def readiness_reason(self) -> str:
        party_count = len(self.active_party_sources())
        if party_count >= self.source_minimum:
            return (
                f"ready: {party_count} aktív pártpreferencia-forrás "
                f"(minimum: {self.source_minimum})"
            )
        return (
            f"not-ready: csak {party_count} aktív pártpreferencia-forrás "
            f"(minimum: {self.source_minimum})"
        )


# ---------------------------------------------------------------------
# Meta / országkör felismerés
# ---------------------------------------------------------------------

def load_repo_meta(meta_path: Path = META_PATH) -> Dict[str, Any]:
    if not meta_path.exists():
        return {}

    try:
        with meta_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {}


def infer_tracked_countries(meta: Optional[Dict[str, Any]] = None) -> List[str]:
    """
    Elsődlegesen a repo saját western_balkans_core listáját használjuk.
    Ha ez hiányzik, fallback a klasszikus WB6.
    Külső ENV felülírás támogatott:
      POLL_COUNTRIES="Serbia,Kosovo,Albania"
    """
    env_override = os.getenv("POLL_COUNTRIES", "").strip()
    if env_override:
        countries = [x.strip() for x in env_override.split(",") if x.strip()]
        if countries:
            return countries

    meta = meta or load_repo_meta()

    core = meta.get("western_balkans_core")
    if isinstance(core, list) and core:
        return core

    return [
        "Albania",
        "Bosnia and Herzegovina",
        "Kosovo",
        "Montenegro",
        "North Macedonia",
        "Serbia",
    ]


# ---------------------------------------------------------------------
# Forrás-regiszter
# ---------------------------------------------------------------------

def shared_context_sources_for(country: str) -> List[PollSource]:
    """
    Közös, regionális kontextusforrások.
    Ezek nem feltétlenül adnak közvetlen pártpreferencia-adatot,
    de jók háttértrendhez, bizalmi/attitűd kontextushoz.
    """
    sources: List[PollSource] = []

    western_balkans_six = {
        "Albania",
        "Bosnia and Herzegovina",
        "Kosovo",
        "Montenegro",
        "North Macedonia",
        "Serbia",
    }

    if country in western_balkans_six:
        sources.append(
            PollSource(
                source_id="iri_wb_regional_poll",
                source_name="International Republican Institute – Western Balkans Regional Poll",
                category="regional",
                country=country,
                homepage="https://www.iri.org/resources/western-balkans-regional-poll-february-march-2024-full/",
                supports_party_polling=False,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=180,
                priority=90,
                active=True,
                confidence="high",
                notes=(
                    "Regionális közvélemény-kutatás; elsősorban attitűd/bizalom/közpolitikai "
                    "kontextusra, nem közvetlen pártpreferencia-aggregációra."
                ),
                tags=["regional", "context", "wb6"],
            )
        )

        sources.append(
            PollSource(
                source_id="rcc_balkan_barometer",
                source_name="RCC – Balkan Barometer",
                category="regional",
                country=country,
                homepage="https://www.rcc.int/balkanbarometer/home",
                supports_party_polling=False,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=365,
                priority=80,
                active=True,
                confidence="high",
                notes=(
                    "Éves regionális survey; társadalmi, gazdasági és intézményi bizalom "
                    "szempontjából erős kontextusforrás."
                ),
                tags=["regional", "context", "wb6"],
            )
        )

    return sources


def country_specific_sources(country: str) -> List[PollSource]:
    """
    Ország-specifikus, később scrape-elhető vagy importálható források.

    Megjegyzés:
    - Itt szándékosan külön választjuk a 'party polling' és a 'context' forrásokat.
    - Ahol még nincs stabil, megbízható, könnyen scrape-elhető lokális forrás, ott
      bootstrap jelleggel hagyunk alacsonyabb bizalmú / inaktív forrást.
    """
    c = country

    mapping: Dict[str, List[PollSource]] = {
        "Serbia": [
            PollSource(
                source_id="europe_elects_serbia",
                source_name="Europe Elects – Serbia",
                category="aggregator",
                country=c,
                homepage="https://europeelects.eu/serbia/",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="custom_html",
                polling_frequency_hint_days=14,
                priority=100,
                active=True,
                confidence="high",
                notes="Elsődleges aggregátor jellegű országoldal.",
                tags=["aggregator", "party-polling", "trend"],
            ),
            PollSource(
                source_id="serbia_elects_ewb",
                source_name="Serbia Elects / European Western Balkans",
                category="aggregator",
                country=c,
                homepage="https://serbiaelects.europeanwesternbalkans.com/",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="custom_html",
                polling_frequency_hint_days=14,
                priority=85,
                active=True,
                confidence="medium",
                notes="Szerbia-választási fókuszú oldal, poll-tartalomhoz használható lehet.",
                tags=["aggregator", "serbia", "party-polling"],
            ),
            PollSource(
                source_id="wikipedia_serbia_polling_bootstrap",
                source_name="Wikipedia – Opinion polling for the next Serbian parliamentary election",
                category="bootstrap",
                country=c,
                homepage="https://en.wikipedia.org/wiki/Opinion_polling_for_the_next_Serbian_parliamentary_election",
                supports_party_polling=True,
                supports_trend_context=False,
                parser_kind="html_table",
                polling_frequency_hint_days=14,
                priority=40,
                active=False,
                confidence="low",
                notes=(
                    "Átmeneti bootstrap forrás lehet, de nem elsődleges. "
                    "Csak addig ajánlott, amíg stabil helyi forrásokat nem kötünk be."
                ),
                tags=["bootstrap", "poll-table"],
            ),
        ],
        "Montenegro": [
            PollSource(
                source_id="europe_elects_montenegro",
                source_name="Europe Elects – Montenegro",
                category="aggregator",
                country=c,
                homepage="https://europeelects.eu/montenegro/",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="custom_html",
                polling_frequency_hint_days=14,
                priority=100,
                active=True,
                confidence="high",
                notes="Elsődleges aggregátor jellegű országoldal.",
                tags=["aggregator", "party-polling", "trend"],
            ),
            PollSource(
                source_id="cedem_montenegro",
                source_name="CEDEM – Montenegro political public opinion research",
                category="local",
                country=c,
                homepage="https://www.cedem.me/en/research/",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="custom_html",
                polling_frequency_hint_days=30,
                priority=95,
                active=True,
                confidence="high",
                notes="Fontos helyi, közvetlen montenegrói kutatási forrás.",
                tags=["local", "polling", "montenegro"],
            ),
            PollSource(
                source_id="europe_elects_montenegro_pdf_archive",
                source_name="Europe Elects – Montenegro PDF / archive coverage",
                category="aggregator",
                country=c,
                homepage="https://europeelects.eu/2023/04/20/exclusive-montenegro-earthquake-poll/",
                supports_party_polling=True,
                supports_trend_context=False,
                parser_kind="disabled",
                polling_frequency_hint_days=30,
                priority=50,
                active=False,
                confidence="medium",
                notes=(
                    "Archivált vagy cikkes megjelenések; később külön parserrel "
                    "vagy kézi importtal lehet hasznos."
                ),
                tags=["archive", "article"],
            ),
        ],
        "Bosnia and Herzegovina": [
            PollSource(
                source_id="europe_elects_bosnia",
                source_name="Europe Elects – Bosnia and Herzegovina",
                category="aggregator",
                country=c,
                homepage="https://europeelects.eu/bosnia-and-herzegovina/",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="custom_html",
                polling_frequency_hint_days=21,
                priority=100,
                active=True,
                confidence="high",
                notes="Elsődleges aggregátor jellegű országoldal.",
                tags=["aggregator", "party-polling", "trend"],
            ),
            PollSource(
                source_id="bosnia_local_pollster_placeholder",
                source_name="Bosnia local pollster placeholder",
                category="local",
                country=c,
                homepage="",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=30,
                priority=70,
                active=False,
                confidence="low",
                notes="Itt a következő körben konkrét helyi boszniai forrást kötünk be.",
                tags=["placeholder", "needs-source"],
            ),
        ],
        "Kosovo": [
            PollSource(
                source_id="europe_elects_kosovo",
                source_name="Europe Elects – Kosovo",
                category="aggregator",
                country=c,
                homepage="https://europeelects.eu/kosovo/",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="custom_html",
                polling_frequency_hint_days=21,
                priority=100,
                active=True,
                confidence="high",
                notes="Elsődleges aggregátor jellegű országoldal.",
                tags=["aggregator", "party-polling", "trend"],
            ),
            PollSource(
                source_id="kosovo_local_pollster_placeholder",
                source_name="Kosovo local pollster placeholder",
                category="local",
                country=c,
                homepage="",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=30,
                priority=70,
                active=False,
                confidence="low",
                notes="Következő körben konkrét koszovói helyi forrás kerül ide.",
                tags=["placeholder", "needs-source"],
            ),
        ],
        "North Macedonia": [
            PollSource(
                source_id="europe_elects_north_macedonia",
                source_name="Europe Elects – North Macedonia",
                category="aggregator",
                country=c,
                homepage="https://europeelects.eu/northmacedonia/",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="custom_html",
                polling_frequency_hint_days=21,
                priority=100,
                active=True,
                confidence="high",
                notes="Elsődleges aggregátor jellegű országoldal.",
                tags=["aggregator", "party-polling", "trend"],
            ),
            PollSource(
                source_id="north_macedonia_local_pollster_placeholder",
                source_name="North Macedonia local pollster placeholder",
                category="local",
                country=c,
                homepage="",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=30,
                priority=70,
                active=False,
                confidence="low",
                notes="Következő körben konkrét észak-macedón helyi forrás kerül ide.",
                tags=["placeholder", "needs-source"],
            ),
        ],
        "Albania": [
            PollSource(
                source_id="europe_elects_albania",
                source_name="Europe Elects – Albania",
                category="aggregator",
                country=c,
                homepage="https://europeelects.eu/albania/",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="custom_html",
                polling_frequency_hint_days=21,
                priority=100,
                active=True,
                confidence="high",
                notes="Elsődleges aggregátor jellegű országoldal.",
                tags=["aggregator", "party-polling", "trend"],
            ),
            PollSource(
                source_id="albania_local_pollster_placeholder",
                source_name="Albania local pollster placeholder",
                category="local",
                country=c,
                homepage="",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=30,
                priority=70,
                active=False,
                confidence="low",
                notes="Következő körben konkrét albán helyi forrást kötünk be.",
                tags=["placeholder", "needs-source"],
            ),
        ],
        # Ha később a szélesebb országlistára is kiterjeszted:
        "Croatia": [
            PollSource(
                source_id="croatia_local_pollster_placeholder",
                source_name="Croatia local pollster placeholder",
                category="local",
                country=c,
                homepage="",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=21,
                priority=80,
                active=False,
                confidence="low",
                notes="Későbbi kiterjesztéshez.",
                tags=["placeholder", "needs-source"],
            ),
        ],
        "Bulgaria": [
            PollSource(
                source_id="bulgaria_local_pollster_placeholder",
                source_name="Bulgaria local pollster placeholder",
                category="local",
                country=c,
                homepage="",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=21,
                priority=80,
                active=False,
                confidence="low",
                notes="Későbbi kiterjesztéshez.",
                tags=["placeholder", "needs-source"],
            ),
        ],
        "Romania": [
            PollSource(
                source_id="romania_local_pollster_placeholder",
                source_name="Romania local pollster placeholder",
                category="local",
                country=c,
                homepage="",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=21,
                priority=80,
                active=False,
                confidence="low",
                notes="Későbbi kiterjesztéshez.",
                tags=["placeholder", "needs-source"],
            ),
        ],
        "Slovenia": [
            PollSource(
                source_id="slovenia_local_pollster_placeholder",
                source_name="Slovenia local pollster placeholder",
                category="local",
                country=c,
                homepage="",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=21,
                priority=80,
                active=False,
                confidence="low",
                notes="Későbbi kiterjesztéshez.",
                tags=["placeholder", "needs-source"],
            ),
        ],
        "Greece": [
            PollSource(
                source_id="greece_local_pollster_placeholder",
                source_name="Greece local pollster placeholder",
                category="local",
                country=c,
                homepage="",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=21,
                priority=80,
                active=False,
                confidence="low",
                notes="Későbbi kiterjesztéshez.",
                tags=["placeholder", "needs-source"],
            ),
        ],
        "Hungary": [
            PollSource(
                source_id="hungary_local_pollster_placeholder",
                source_name="Hungary local pollster placeholder",
                category="local",
                country=c,
                homepage="",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=21,
                priority=80,
                active=False,
                confidence="low",
                notes="Későbbi kiterjesztéshez.",
                tags=["placeholder", "needs-source"],
            ),
        ],
        "Turkey": [
            PollSource(
                source_id="turkey_local_pollster_placeholder",
                source_name="Turkey local pollster placeholder",
                category="local",
                country=c,
                homepage="",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=21,
                priority=80,
                active=False,
                confidence="low",
                notes="Későbbi kiterjesztéshez.",
                tags=["placeholder", "needs-source"],
            ),
        ],
        "Moldova": [
            PollSource(
                source_id="moldova_local_pollster_placeholder",
                source_name="Moldova local pollster placeholder",
                category="local",
                country=c,
                homepage="",
                supports_party_polling=True,
                supports_trend_context=True,
                parser_kind="disabled",
                polling_frequency_hint_days=21,
                priority=80,
                active=False,
                confidence="low",
                notes="Későbbi kiterjesztéshez.",
                tags=["placeholder", "needs-source"],
            ),
        ],
    }

    return mapping.get(country, [])


def build_country_config(country: str) -> CountryPollConfig:
    sources = []
    sources.extend(country_specific_sources(country))
    sources.extend(shared_context_sources_for(country))

    return CountryPollConfig(
        country=country,
        enabled=True,
        region_group="western_balkans" if country in {
            "Albania",
            "Bosnia and Herzegovina",
            "Kosovo",
            "Montenegro",
            "North Macedonia",
            "Serbia",
        } else "extended_balkans",
        election_type="parliamentary",
        source_minimum=3,
        parties_namespace=country.lower().replace(" ", "_").replace("-", "_"),
        sources=sources,
    )


def build_registry(countries: Optional[List[str]] = None) -> Dict[str, CountryPollConfig]:
    countries = countries or infer_tracked_countries()
    return {country: build_country_config(country) for country in countries}


# ---------------------------------------------------------------------
# Export / reporting
# ---------------------------------------------------------------------

def registry_to_plain_dict(registry: Dict[str, CountryPollConfig]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    for country, cfg in registry.items():
        out[country] = {
            "country": cfg.country,
            "enabled": cfg.enabled,
            "region_group": cfg.region_group,
            "election_type": cfg.election_type,
            "source_minimum": cfg.source_minimum,
            "parties_namespace": cfg.parties_namespace,
            "ready_for_aggregation": cfg.ready_for_aggregation(),
            "readiness_reason": cfg.readiness_reason(),
            "active_party_source_count": len(cfg.active_party_sources()),
            "active_context_source_count": len(cfg.active_context_sources()),
            "sources": [asdict(src) for src in cfg.sources],
        }

    return out


def print_human_summary(registry: Dict[str, CountryPollConfig]) -> None:
    print("=== Balkan Party Poll Registry ===")
    for country, cfg in registry.items():
        print(f"\n[{country}]")
        print(f"  enabled: {cfg.enabled}")
        print(f"  ready:   {cfg.ready_for_aggregation()}")
        print(f"  reason:  {cfg.readiness_reason()}")
        print(f"  party-sources:   {len(cfg.active_party_sources())}")
        print(f"  context-sources: {len(cfg.active_context_sources())}")

        for src in cfg.sources:
            status = "ACTIVE" if src.active else "OFF"
            pp = "party" if src.supports_party_polling else "context"
            print(
                f"   - {status:6} | {src.category:10} | {pp:7} | "
                f"{src.source_name}"
            )


def export_registry_json(
    output_path: Path,
    registry: Dict[str, CountryPollConfig]
) -> None:
    payload = registry_to_plain_dict(registry)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def main() -> None:
    registry = build_registry()

    export_target = os.getenv("POLL_REGISTRY_EXPORT", "").strip()
    if export_target:
        export_registry_json(Path(export_target), registry)

    print_human_summary(registry)


if __name__ == "__main__":
    main()
