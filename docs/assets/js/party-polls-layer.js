/* party-polls-layer.js
 *
 * Feladat:
 * - betölti a party_poll_aggregates.json fájlt
 * - országnevek alapján indexeli az adatokat
 * - egységes API-t ad a térképhez / popuphoz / tooltiphez
 * - nem írja felül a meglévő map logikát, hanem ráépül
 *
 * Használat:
 *   <script src="./assets/js/party-polls-layer.js"></script>
 *
 * Ez a fájl önmagában nem nyúl a meglévő rétegekhez.
 * A következő körben majd a meglévő index.html-be lehet bekötni.
 */

(function () {
  "use strict";

  const DEFAULT_CONFIG = {
    dataUrl: "./data/processed/polls/party_poll_aggregates.json",
    debug: false
  };

  const COUNTRY_ALIASES = {
    "albania": "Albania",
    "bosnia": "Bosnia and Herzegovina",
    "bosnia and herzegovina": "Bosnia and Herzegovina",
    "bih": "Bosnia and Herzegovina",
    "kosovo": "Kosovo",
    "montenegro": "Montenegro",
    "north macedonia": "North Macedonia",
    "macedonia": "North Macedonia",
    "serbia": "Serbia",
    "croatia": "Croatia",
    "slovenia": "Slovenia",
    "romania": "Romania",
    "bulgaria": "Bulgaria",
    "greece": "Greece",
    "hungary": "Hungary",
    "turkey": "Turkey",
    "moldova": "Moldova"
  };

  function logDebug(enabled, ...args) {
    if (enabled) {
      console.log("[party-polls-layer]", ...args);
    }
  }

  function normalizeCountryName(value) {
    const text = String(value || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, " ");

    if (!text) {
      return "";
    }

    return COUNTRY_ALIASES[text] || value;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function trendSymbol(direction) {
    switch (direction) {
      case "up":
        return "↑";
      case "down":
        return "↓";
      case "flat":
        return "→";
      default:
        return "?";
    }
  }

  function trendLabel(direction) {
    switch (direction) {
      case "up":
        return "emelkedő";
      case "down":
        return "csökkenő";
      case "flat":
        return "stagnáló";
      default:
        return "ismeretlen";
    }
  }

  function readinessLabel(ready) {
    return ready ? "Megjeleníthető" : "Még nem elég erős";
  }

  function formatPercent(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      return "—";
    }
    return `${Number(value).toFixed(1)}%`;
  }

  function safeArray(value) {
    return Array.isArray(value) ? value : [];
  }

  function buildCountryIndex(payload) {
    const countries = safeArray(payload && payload.countries);
    const index = new Map();

    for (const item of countries) {
      const key = normalizeCountryName(item && item.country);
      if (!key) {
        continue;
      }
      index.set(key, item);
    }

    return index;
  }

  function buildLeaderText(countryData) {
    const leader = countryData && countryData.leader;
    if (!leader) {
      return "Nincs vezető párt adat.";
    }

    const party = escapeHtml(leader.party || "Ismeretlen");
    const score = formatPercent(leader.weighted_average);
    const direction = trendLabel(leader.trend_direction);
    const symbol = trendSymbol(leader.trend_direction);

    return `${party} (${score}) ${symbol} ${direction}`;
  }

  function buildTopPartiesHtml(countryData, limit = 5) {
    const parties = safeArray(countryData && countryData.parties).slice(0, limit);

    if (!parties.length) {
      return `<div class="party-polls-empty">Nincs feldolgozott pártadat.</div>`;
    }

    const rows = parties.map((party) => {
      const partyName = escapeHtml(party.party || "Ismeretlen párt");
      const weighted = formatPercent(party.weighted_average);
      const simple = formatPercent(party.simple_average);
      const trendDir = party && party.trend ? party.trend.direction : "unknown";
      const symbol = trendSymbol(trendDir);
      const trendTxt = trendLabel(trendDir);
      const sourceCount = Number(party.source_count || 0);
      const pollCount = Number(party.poll_count || 0);

      return `
        <tr>
          <td>${partyName}</td>
          <td>${weighted}</td>
          <td>${simple}</td>
          <td>${symbol} ${escapeHtml(trendTxt)}</td>
          <td>${sourceCount}</td>
          <td>${pollCount}</td>
        </tr>
      `;
    });

    return `
      <div class="party-polls-table-wrap">
        <table class="party-polls-table">
          <thead>
            <tr>
              <th>Párt</th>
              <th>Súlyozott</th>
              <th>Egyszerű átlag</th>
              <th>Trend</th>
              <th>Forrás</th>
              <th>Mérés</th>
            </tr>
          </thead>
          <tbody>
            ${rows.join("")}
          </tbody>
        </table>
      </div>
    `;
  }

  function buildSourcesHtml(countryData) {
    const status = countryData && countryData.status ? countryData.status : {};
    const sources = safeArray(status.distinct_sources);

    if (!sources.length) {
      return `<div class="party-polls-sources-empty">Nincs forráslista.</div>`;
    }

    return `
      <div class="party-polls-sources">
        ${sources
          .map((source) => `<span class="party-polls-source-chip">${escapeHtml(source)}</span>`)
          .join("")}
      </div>
    `;
  }

  function buildCountrySummaryHtml(countryName, countryData) {
    if (!countryData) {
      return `
        <div class="party-polls-card">
          <div class="party-polls-title">${escapeHtml(countryName)}</div>
          <div class="party-polls-status not-ready">Nincs közvélemény-kutatási adat.</div>
        </div>
      `;
    }

    const status = countryData.status || {};
    const leader = countryData.leader || null;
    const ready = !!status.ready_for_display;
    const latestPollDate = escapeHtml(status.latest_poll_date || "—");
    const distinctSourceCount = Number(status.distinct_source_count || 0);
    const freshPollCount = Number(status.fresh_poll_count || 0);

    return `
      <div class="party-polls-card">
        <div class="party-polls-title">${escapeHtml(countryName)}</div>

        <div class="party-polls-status ${ready ? "ready" : "not-ready"}">
          ${escapeHtml(readinessLabel(ready))}
        </div>

        <div class="party-polls-meta">
          <div><strong>Vezető párt:</strong> ${leader ? buildLeaderText(countryData) : "Nincs adat"}</div>
          <div><strong>Utolsó mérés:</strong> ${latestPollDate}</div>
          <div><strong>Eltérő források:</strong> ${distinctSourceCount}</div>
          <div><strong>Felhasznált mérések:</strong> ${freshPollCount}</div>
        </div>

        <div class="party-polls-section-title">Top pártok</div>
        ${buildTopPartiesHtml(countryData, 5)}

        <div class="party-polls-section-title">Forrás-azonosítók</div>
        ${buildSourcesHtml(countryData)}
      </div>
    `;
  }

  function buildCompactTooltipHtml(countryName, countryData) {
    if (!countryData) {
      return `
        <div class="party-polls-tooltip">
          <div><strong>${escapeHtml(countryName)}</strong></div>
          <div>Nincs poll adat</div>
        </div>
      `;
    }

    const status = countryData.status || {};
    const leader = countryData.leader || null;
    const ready = !!status.ready_for_display;

    return `
      <div class="party-polls-tooltip">
        <div><strong>${escapeHtml(countryName)}</strong></div>
        <div>Állapot: ${escapeHtml(readinessLabel(ready))}</div>
        <div>Vezető: ${
          leader
            ? `${escapeHtml(leader.party || "Ismeretlen")} (${formatPercent(leader.weighted_average)})`
            : "Nincs adat"
        }</div>
        <div>Trend: ${
          leader
            ? `${trendSymbol(leader.trend_direction)} ${escapeHtml(trendLabel(leader.trend_direction))}`
            : "—"
        }</div>
        <div>Források: ${Number(status.distinct_source_count || 0)}</div>
      </div>
    `;
  }

  function injectDefaultStyles() {
    if (document.getElementById("party-polls-layer-styles")) {
      return;
    }

    const style = document.createElement("style");
    style.id = "party-polls-layer-styles";
    style.textContent = `
      .party-polls-card {
        font-family: Arial, sans-serif;
        font-size: 13px;
        line-height: 1.45;
        min-width: 320px;
        max-width: 520px;
      }

      .party-polls-title {
        font-size: 16px;
        font-weight: 700;
        margin-bottom: 8px;
      }

      .party-polls-status {
        display: inline-block;
        padding: 4px 8px;
        border-radius: 6px;
        font-size: 12px;
        font-weight: 700;
        margin-bottom: 10px;
      }

      .party-polls-status.ready {
        background: rgba(0, 128, 0, 0.12);
      }

      .party-polls-status.not-ready {
        background: rgba(180, 120, 0, 0.16);
      }

      .party-polls-meta {
        display: grid;
        gap: 4px;
        margin-bottom: 12px;
      }

      .party-polls-section-title {
        margin-top: 10px;
        margin-bottom: 6px;
        font-weight: 700;
      }

      .party-polls-table-wrap {
        overflow-x: auto;
      }

      .party-polls-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
      }

      .party-polls-table th,
      .party-polls-table td {
        text-align: left;
        padding: 6px 4px;
        border-bottom: 1px solid rgba(120, 120, 120, 0.2);
        vertical-align: top;
      }

      .party-polls-sources {
        display: flex;
        flex-wrap: wrap;
        gap: 6px;
      }

      .party-polls-source-chip {
        display: inline-block;
        padding: 4px 8px;
        border-radius: 999px;
        background: rgba(80, 80, 80, 0.12);
        font-size: 11px;
      }

      .party-polls-tooltip {
        font-family: Arial, sans-serif;
        font-size: 12px;
        line-height: 1.35;
        min-width: 180px;
      }

      .party-polls-empty,
      .party-polls-sources-empty {
        opacity: 0.8;
        font-size: 12px;
      }
    `;
    document.head.appendChild(style);
  }

  class PartyPollsLayer {
    constructor(config = {}) {
      this.config = { ...DEFAULT_CONFIG, ...config };
      this.payload = null;
      this.countryIndex = new Map();
      this.loaded = false;
      this.loadingPromise = null;
    }

    async load() {
      if (this.loaded) {
        return this.payload;
      }

      if (this.loadingPromise) {
        return this.loadingPromise;
      }

      this.loadingPromise = fetch(this.config.dataUrl, {
        cache: "no-store"
      })
        .then((response) => {
          if (!response.ok) {
            throw new Error(`HTTP ${response.status} while loading ${this.config.dataUrl}`);
          }
          return response.json();
        })
        .then((payload) => {
          this.payload = payload || {};
          this.countryIndex = buildCountryIndex(this.payload);
          this.loaded = true;
          injectDefaultStyles();
          logDebug(this.config.debug, "loaded payload", this.payload);
          return this.payload;
        })
        .catch((error) => {
          console.error("[party-polls-layer] load failed:", error);
          throw error;
        });

      return this.loadingPromise;
    }

    isLoaded() {
      return this.loaded;
    }

    getPayload() {
      return this.payload;
    }

    getCountryData(countryName) {
      const normalized = normalizeCountryName(countryName);
      return this.countryIndex.get(normalized) || null;
    }

    hasCountryData(countryName) {
      return !!this.getCountryData(countryName);
    }

    getLeader(countryName) {
      const data = this.getCountryData(countryName);
      return data && data.leader ? data.leader : null;
    }

    getTopParties(countryName, limit = 5) {
      const data = this.getCountryData(countryName);
      if (!data || !Array.isArray(data.parties)) {
        return [];
      }
      return data.parties.slice(0, limit);
    }

    getReadiness(countryName) {
      const data = this.getCountryData(countryName);
      if (!data || !data.status) {
        return {
          ready_for_display: false,
          readiness_reason: "Nincs adat"
        };
      }
      return {
        ready_for_display: !!data.status.ready_for_display,
        readiness_reason: data.status.readiness_reason || ""
      };
    }

    buildPopupHtml(countryName) {
      const data = this.getCountryData(countryName);
      return buildCountrySummaryHtml(countryName, data);
    }

    buildTooltipHtml(countryName) {
      const data = this.getCountryData(countryName);
      return buildCompactTooltipHtml(countryName, data);
    }

    attachToLeafletGeoJson(geoJsonLayer, options = {}) {
      if (!geoJsonLayer || typeof geoJsonLayer.eachLayer !== "function") {
        throw new Error("attachToLeafletGeoJson: invalid geoJsonLayer");
      }

      const countryPropertyCandidates = safeArray(options.countryPropertyCandidates).length
        ? options.countryPropertyCandidates
        : ["name", "NAME", "admin", "ADMIN", "country", "COUNTRY"];

      geoJsonLayer.eachLayer((layer) => {
        const feature = layer && layer.feature ? layer.feature : {};
        const props = feature.properties || {};

        let countryName = "";
        for (const propName of countryPropertyCandidates) {
          if (props[propName]) {
            countryName = String(props[propName]);
            break;
          }
        }

        if (!countryName) {
          return;
        }

        if (typeof layer.bindTooltip === "function") {
          layer.bindTooltip(this.buildTooltipHtml(countryName), {
            sticky: true,
            direction: "auto",
            opacity: 0.95
          });
        }

        if (typeof layer.bindPopup === "function") {
          layer.bindPopup(this.buildPopupHtml(countryName), {
            maxWidth: 560
          });
        }

        if (typeof layer.on === "function") {
          layer.on("mouseover", () => {
            if (typeof layer.setStyle === "function") {
              layer.setStyle({
                weight: 2
              });
            }
          });

          layer.on("mouseout", () => {
            if (typeof geoJsonLayer.resetStyle === "function") {
              geoJsonLayer.resetStyle(layer);
            }
          });
        }
      });
    }
  }

  window.PartyPollsLayer = PartyPollsLayer;
})();
