TRUSTED_RSS_FEEDS = [
    {
        "id": "euronews_world",
        "name": "Euronews",
        "url": "https://www.euronews.com/rss?level=theme&name=news",
        "weight": 0.92,
        "language": "en",
        "scope": ["world", "europe", "balkans", "security", "politics"],
        "trusted": True,
    },
    {
        "id": "guardian_world",
        "name": "The Guardian",
        "url": "https://www.theguardian.com/world/rss",
        "weight": 0.95,
        "language": "en",
        "scope": ["world", "europe", "security", "politics"],
        "trusted": True,
    },
    {
        "id": "guardian_europe",
        "name": "The Guardian Europe",
        "url": "https://www.theguardian.com/world/europe-news/rss",
        "weight": 0.95,
        "language": "en",
        "scope": ["europe", "balkans", "security", "politics"],
        "trusted": True,
    },
    {
        "id": "dw_world",
        "name": "DW",
        "url": "https://rss.dw.com/rdf/rss-en-world",
        "weight": 0.92,
        "language": "en",
        "scope": ["world", "europe", "security", "politics"],
        "trusted": True,
    },
    {
        "id": "dw_europe",
        "name": "DW Europe",
        "url": "https://rss.dw.com/rdf/rss-en-eu",
        "weight": 0.93,
        "language": "en",
        "scope": ["europe", "balkans", "security", "politics"],
        "trusted": True,
    },
    {
        "id": "politico_eu",
        "name": "Politico Europe",
        "url": "https://www.politico.eu/feed/",
        "weight": 0.88,
        "language": "en",
        "scope": ["eu", "nato", "policy", "politics"],
        "trusted": True,
    },
    {
        "id": "cnn_world",
        "name": "CNN World",
        "url": "http://rss.cnn.com/rss/edition_world.rss",
        "weight": 0.80,
        "language": "en",
        "scope": ["world", "security", "politics"],
        "trusted": True,
    },
]

BALKAN_COUNTRY_KEYWORDS = {
    "Albania": ["albania", "tirana", "albanian"],
    "Bosnia and Herzegovina": ["bosnia", "sarajevo", "republika srpska", "bih", "bosnian"],
    "Bulgaria": ["bulgaria", "sofia", "bulgarian"],
    "Croatia": ["croatia", "zagreb", "croatian"],
    "Greece": ["greece", "athens", "greek"],
    "Kosovo": ["kosovo", "pristina", "priština", "kosovar"],
    "Montenegro": ["montenegro", "podgorica", "montenegrin"],
    "North Macedonia": ["north macedonia", "skopje", "macedonia", "macedonian"],
    "Romania": ["romania", "bucharest", "romanian"],
    "Serbia": ["serbia", "belgrade", "serbian"],
    "Slovenia": ["slovenia", "ljubljana", "slovenian"],
    "Turkey": ["turkey", "ankara", "istanbul", "turkish"],
    "Moldova": ["moldova", "chisinau", "chișinău", "moldovan"],
}

DIMENSION_KEYWORDS = {
    "political": [
        "election", "government", "parliament", "president", "prime minister",
        "coalition", "opposition", "vote", "ballot", "minister", "resign"
    ],
    "military": [
        "military", "army", "troops", "defence", "defense", "nato", "exercise",
        "drone", "airstrike", "weapon", "armed forces", "missile"
    ],
    "policing": [
        "police", "arrest", "raid", "investigation", "court", "prosecutor",
        "corruption", "crime", "smuggling", "detention"
    ],
    "migration": [
        "migrant", "migration", "refugee", "asylum", "border crossing",
        "border", "smuggling route", "detention camp"
    ],
    "social": [
        "protest", "strike", "demonstration", "riot", "student", "union",
        "teachers", "workers", "civil society"
    ],
    "infrastructure": [
        "energy", "pipeline", "grid", "electricity", "gas", "oil",
        "port", "rail", "bridge", "airport", "blackout", "infrastructure"
    ],
}

EXCLUDE_KEYWORDS = [
    "sport", "football", "soccer", "tennis", "basketball", "celebrity", "fashion",
    "movie", "music", "entertainment", "lifestyle"
]
