<!doctype html>
<html lang="hu">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Balkán Biztonsági Monitor – Feszültség</title>

  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>

  <style>
    body { margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; }
    #map { height: 100vh; width: 100vw; }

    .panel {
      position: absolute; top: 12px; left: 12px; z-index: 999;
      background: rgba(255,255,255,0.95); padding: 12px 12px; border-radius: 12px;
      box-shadow: 0 6px 24px rgba(0,0,0,0.12);
      max-width: 460px;
    }
    .panel h1 { font-size: 16px; margin: 0 0 6px; }
    .panel small { color:#444; display:block; margin-bottom:10px; line-height:1.25; }

    .summary {
      border-radius: 12px;
      background: #fafafa;
      padding: 10px 10px;
      margin: 8px 0 10px;
      border: 1px solid #eee;
    }
    .summary h2 { font-size: 13px; margin: 0 0 6px; }
    .summary ul { margin: 0; padding-left: 18px; }
    .summary li { font-size: 12px; color:#333; margin: 4px 0; line-height: 1.25; }

    .row { display:flex; gap:8px; align-items:center; margin:8px 0; }
    .row label { font-size: 13px; }
    .row input { transform: translateY(1px); }

    .badge { display:inline-block; padding:2px 8px; border-radius:999px; background:#f2f2f2; font-size:12px; margin-right:6px; }
    .legend { font-size: 12px; color:#333; line-height: 1.35; margin-top:10px; }

    .hotspots { margin-top:10px; }
    .hotspots h2 { font-size: 13px; margin: 10px 0 6px; }
    .list { list-style:none; padding:0; margin:0; }
    .item {
      display:flex; justify-content:space-between; gap:10px;
      padding:6px 8px; border-radius:10px; cursor:pointer;
    }
    .item:hover { background:#f2f2f2; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size: 12px; color:#333; }
  </style>
</head>
<body>
  <div id="map"></div>

  <div class="panel">
    <h1>Balkán – Feszültség monitor</h1>
    <small id="meta">Adatok betöltése...</small>

    <div class="summary">
      <h2 id="sumTitle">Napi kivonat</h2>
      <ul id="sumList"><li>Betöltés...</li></ul>
    </div>

    <div class="row">
      <input type="checkbox" id="layerHot" checked>
      <label for="layerHot"><span class="badge">HOTSPOT</span> feszültség (rács)</label>
    </div>

    <div class="row">
      <input type="checkbox" id="layerGdelt" checked>
      <label for="layerGdelt"><span class="badge">GDELT</span> jelzések</label>
    </div>

    <div class="row">
      <input type="checkbox" id="layerUsgs">
      <label for="layerUsgs"><span class="badge">USGS</span> földrengések</label>
    </div>

    <div class="row">
      <input type="checkbox" id="layerGdacs">
      <label for="layerGdacs"><span class="badge">GDACS</span> riasztások</label>
    </div>

    <div class="hotspots">
      <h2>Top hotspotok (súlyozott)</h2>
      <ul class="list" id="hotList"></ul>
    </div>

    <div class="legend">
      A HOTSPOT rács a jelzések frissességét és típusát súlyozza (híralapú jelzések nagyobb súlyt kapnak).
      A “napi kivonat” automatikusan generált, OSINT jellegű összegzés.
    </div>
  </div>

  <script>
    const map = L.map('map', { zoomControl: true }).setView([44.2, 20.6], 6);

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 18,
      attribution: '&copy; OpenStreetMap'
    }).addTo(map);

    function esc(s) {
      return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }

    function fmtBudapest(isoUtcString) {
      if (!isoUtcString) return null;
      const d = new Date(isoUtcString);
      if (isNaN(d.getTime())) return null;
      const bud = new Intl.DateTimeFormat('hu-HU', {
        timeZone: 'Europe/Budapest',
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit'
      }).format(d);
      const utc = new Intl.DateTimeFormat('hu-HU', {
        timeZone: 'UTC',
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', second: '2-digit'
      }).format(d);
      return { bud, utc };
    }

    function makePopup(props) {
      const t = props.time ? `<div><b>Idő:</b> ${esc(props.time)}</div>` : '';
      const title = props.title ? `<div style="margin-bottom:6px;"><b>${esc(props.title)}</b></div>` : '';
      const place = props.place ? `<div><b>Hely:</b> ${esc(props.place)}</div>` : '';
      const mag = (props.mag !== undefined && props.mag !== null) ? `<div><b>Magnitúdó:</b> ${esc(props.mag)}</div>` : '';
      const domain = props.domain ? `<div><b>Domain:</b> ${esc(props.domain)}</div>` : '';
      const url = props.url ? `<div style="margin-top:6px;"><a href="${esc(props.url)}" target="_blank" rel="noopener">Forrás</a></div>` : '';
      return `${title}${t}${place}${mag}${domain}${url}`;
    }

    function circleStyle(source) {
      if (source === 'USGS') return { radius: 7, weight: 1, fillOpacity: 0.5 };
      if (source === 'GDACS') return { radius: 8, weight: 1, fillOpacity: 0.5 };
      return { radius: 6, weight: 1, fillOpacity: 0.5 };
    }

    function hotspotRadius(score) {
      const s = Math.max(0, Number(score) || 0);
      return Math.min(22, 6 + s * 6);
    }

    async function loadGeoJson(url, sourceName) {
      const r = await fetch(url, { cache: "no-store" });
      const gj = await r.json();
      return L.geoJSON(gj, {
        pointToLayer: (feature, latlng) => {
          const style = circleStyle(sourceName);
          return L.circleMarker(latlng, style);
        },
        onEachFeature: (feature, layer) => {
          const props = feature.properties || {};
          layer.bindPopup(makePopup(props));
        }
      });
    }

    async function loadHotspotsGeo(url) {
      const r = await fetch(url, { cache: "no-store" });
      const gj = await r.json();
      return L.geoJSON(gj, {
        pointToLayer: (feature, latlng) => {
          const p = feature.properties || {};
          const radius = hotspotRadius(p.score);
          return L.circleMarker(latlng, { radius, weight: 1, fillOpacity: 0.25 });
        },
        onEachFeature: (feature, layer) => {
          const p = feature.properties || {};
          const src = p.sources ? `GDELT:${p.sources.GDELT||0}, USGS:${p.sources.USGS||0}, GDACS:${p.sources.GDACS||0}` : '';
          layer.bindPopup(
            `<div><b>Hotspot cell</b></div>
             <div><b>Score:</b> ${esc(p.score)}</div>
             <div><b>Count:</b> ${esc(p.count)}</div>
             <div class="mono">${esc(src)}</div>`
          );
        }
      });
    }

    function renderHotList(items) {
      const ul = document.getElementById('hotList');
      ul.innerHTML = '';
      for (const it of items) {
        const li = document.createElement('li');
        li.className = 'item';
        li.innerHTML = `
          <div>
            <div class="mono">(${it.lat.toFixed(2)}, ${it.lon.toFixed(2)})</div>
            <div style="font-size:12px;color:#444;">db: ${it.count} | G:${it.sources.GDELT} U:${it.sources.USGS} D:${it.sources.GDACS}</div>
          </div>
          <div class="mono">S=${Number(it.score).toFixed(2)}</div>
        `;
        li.addEventListener('click', () => map.setView([it.lat, it.lon], 9));
        ul.appendChild(li);
      }
      if (!items.length) {
        const li = document.createElement('li');
        li.style.fontSize = '12px';
        li.style.color = '#555';
        li.textContent = 'Nincs elég adat a hotspotokhoz (vagy üres feed).';
        ul.appendChild(li);
      }
    }

    function renderSummary(sum) {
      document.getElementById('sumTitle').textContent = sum?.headline || 'Napi kivonat';
      const ul = document.getElementById('sumList');
      ul.innerHTML = '';
      const bullets = (sum?.bullets || []).slice(0, 6);
      if (!bullets.length) {
        const li = document.createElement('li');
        li.textContent = 'Nincs elérhető kivonat (még nincs friss adat).';
        ul.appendChild(li);
        return;
      }
      for (const b of bullets) {
        const li = document.createElement('li');
        li.textContent = b;
        ul.appendChild(li);
      }
    }

    const layers = { hot: null, gdelt: null, usgs: null, gdacs: null };

    async function init() {
      // meta
      try {
        const metaResp = await fetch('./data/meta.json', { cache: "no-store" });
        const meta = await metaResp.json();

        const t = fmtBudapest(meta.generated_utc);
        const timeText = t ? `Utolsó frissítés (Budapest): ${t.bud} (UTC: ${t.utc})` : `Utolsó frissítés: ismeretlen`;

        document.getElementById('meta').textContent =
          `${timeText} | hotspot cellák: ${meta.counts.hotspot_cells ?? 0} | GDELT: ${meta.counts.gdelt}, USGS: ${meta.counts.usgs}, GDACS: ${meta.counts.gdacs}`;
      } catch (e) {
        document.getElementById('meta').textContent = 'Meta betöltése nem sikerült.';
      }

      // summary
      try {
        const sumResp = await fetch('./data/summary.json', { cache: "no-store" });
        const sum = await sumResp.json();
        renderSummary(sum);
      } catch (e) {
        renderSummary({ headline: 'Napi kivonat', bullets: [] });
      }

      // layers
      layers.hot   = await loadHotspotsGeo('./data/hotspots.geojson');
      layers.gdelt = await loadGeoJson('./data/gdelt.geojson', 'GDELT');
      layers.usgs  = await loadGeoJson('./data/usgs.geojson', 'USGS');
      layers.gdacs = await loadGeoJson('./data/gdacs.geojson', 'GDACS');

      layers.hot.addTo(map);
      layers.gdelt.addTo(map);

      document.getElementById('layerHot').addEventListener('change', (e) => {
        if (e.target.checked) map.addLayer(layers.hot); else map.removeLayer(layers.hot);
      });
      document.getElementById('layerGdelt').addEventListener('change', (e) => {
        if (e.target.checked) map.addLayer(layers.gdelt); else map.removeLayer(layers.gdelt);
      });
      document.getElementById('layerUsgs').addEventListener('change', (e) => {
        if (e.target.checked) map.addLayer(layers.usgs); else map.removeLayer(layers.usgs);
      });
      document.getElementById('layerGdacs').addEventListener('change', (e) => {
        if (e.target.checked) map.addLayer(layers.gdacs); else map.removeLayer(layers.gdacs);
      });

      // top hotspots list
      try {
        const topResp = await fetch('./data/hotspots.json', { cache: "no-store" });
        const top = await topResp.json();
        renderHotList(top.top || []);
      } catch (e) {
        renderHotList([]);
      }
    }

    init();
  </script>
</body>
</html>
