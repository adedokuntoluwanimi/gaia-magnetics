/* =====================================
   DOM HELPERS
===================================== */
function $(id) {
  return document.getElementById(id);
}

/* =====================================
   DOM ELEMENTS
===================================== */
const csvFileInput       = $("csvFile");
const scenarioSelect     = $("scenario");
const spacingInput       = $("spacing");

const xColumnSelect      = $("xColumn");
const yColumnSelect      = $("yColumn");
const valueColumnSelect  = $("valueColumn");

const createJobBtn       = $("createJobBtn");
const downloadBtn        = $("downloadBtn");
const plotBtn            = $("plotBtn");
const jobStatus          = $("jobStatus");

const canvas             = $("plotCanvas");
const ctx                = canvas ? canvas.getContext("2d") : null;

let currentJobId = null;

/* =====================================
   SCENARIO HANDLING
===================================== */
if (scenarioSelect && spacingInput) {
  scenarioSelect.addEventListener("change", () => {
    const isExplicit = normalizeScenario(scenarioSelect.value) === "explicit";
    spacingInput.disabled = isExplicit;
    if (isExplicit) spacingInput.value = "";
  });
}

/* =====================================
   CSV HEADER PARSING
===================================== */
if (csvFileInput) {
  csvFileInput.addEventListener("change", () => {
    const file = csvFileInput.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = () => {
      const headers = reader.result
        .split("\n")[0]
        .split(",")
        .map(h => h.trim());

      populateSelect(xColumnSelect, headers);
      populateSelect(yColumnSelect, headers);
      populateSelect(valueColumnSelect, headers);
    };

    reader.readAsText(file);
  });
}

function populateSelect(select, options) {
  if (!select) return;
  select.innerHTML = "";
  options.forEach(opt => {
    const o = document.createElement("option");
    o.value = opt;
    o.textContent = opt;
    select.appendChild(o);
  });
}

/* =====================================
   SCENARIO NORMALIZATION
===================================== */
function normalizeScenario(v) {
  if (v === "sparse_only" || v === "sparse_geometry") return "sparse";
  return "explicit";
}

/* =====================================
   CREATE JOB
===================================== */
if (createJobBtn) {
  createJobBtn.addEventListener("click", async () => {
    if (!csvFileInput || !csvFileInput.files.length) {
      alert("Upload a CSV first");
      return;
    }

    if (!scenarioSelect || !xColumnSelect || !yColumnSelect || !valueColumnSelect) {
      console.error("Missing required form fields");
      return;
    }

    const scenario = normalizeScenario(scenarioSelect.value);

    jobStatus.textContent = "Running job...";

    const form = new FormData();
    form.append("file", csvFileInput.files[0]);
    form.append("scenario", scenario);
    form.append("x_col", xColumnSelect.value);
    form.append("y_col", yColumnSelect.value);
    form.append("tmi_col", valueColumnSelect.value);

    if (scenario === "sparse") {
      if (!spacingInput || !spacingInput.value) {
        alert("Station spacing is required for sparse geometry");
        jobStatus.textContent = "";
        return;
      }
      form.append("station_spacing", spacingInput.value);
    }

    let res;
    try {
      res = await fetch("/jobs", {
        method: "POST",
        body: form
      });
    } catch (e) {
      console.error("Network error:", e);
      jobStatus.textContent = "";
      return;
    }

    if (!res.ok) {
      console.error("Backend error:", await res.text());
      jobStatus.textContent = "";
      return;
    }

    const data = await res.json();
    currentJobId = data.job_id;

    jobStatus.textContent = "";
    downloadBtn.disabled = false;
    plotBtn.disabled = false;
  });
}

/* =====================================
   DOWNLOAD
===================================== */
if (downloadBtn) {
  downloadBtn.addEventListener("click", () => {
    if (!currentJobId) return;
    window.location.href = `/jobs/${currentJobId}/download`;
  });
}

/* =====================================
   PLOT
===================================== */
if (plotBtn && ctx) {
  plotBtn.addEventListener("click", async () => {
    if (!currentJobId) return;

    const res = await fetch(`/jobs/${currentJobId}/download`);
    const text = await res.text();

    const rows = text.trim().split("\n");
    const header = rows.shift().split(",");

    const idx = {
      d: header.indexOf("d_along"),
      t: header.indexOf("tmi"),
      m: header.indexOf("is_measured"),
    };

    const measured = [];
    const predicted = [];

    rows.forEach(r => {
      const c = r.split(",");
      const p = { x: +c[idx.d], y: +c[idx.t] };
      c[idx.m] === "1" ? measured.push(p) : predicted.push(p);
    });

    drawPlot(measured, predicted);
  });
}

function drawPlot(measured, predicted) {
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  const all = measured.concat(predicted);
  if (!all.length) return;

  const pad = 60;
  const xs = all.map(p => p.x);
  const ys = all.map(p => p.y);

  const xmin = Math.min(...xs);
  const xmax = Math.max(...xs);
  const ymin = Math.min(...ys);
  const ymax = Math.max(...ys);

  const sx = (canvas.width - 2 * pad) / (xmax - xmin || 1);
  const sy = (canvas.height - 2 * pad) / (ymax - ymin || 1);

  const map = p => ({
    x: pad + (p.x - xmin) * sx,
    y: canvas.height - (pad + (p.y - ymin) * sy),
  });

  ctx.strokeStyle = "#aaa";
  ctx.beginPath();
  ctx.moveTo(pad, pad);
  ctx.lineTo(pad, canvas.height - pad);
  ctx.lineTo(canvas.width - pad, canvas.height - pad);
  ctx.stroke();

  ctx.fillText("Distance along traverse (m)", canvas.width / 2 - 90, canvas.height - 20);
  ctx.save();
  ctx.rotate(-Math.PI / 2);
  ctx.fillText("Magnetic value (TMI)", -canvas.height / 2 - 50, 20);
  ctx.restore();

  ctx.fillStyle = "blue";
  measured.forEach(p => {
    const m = map(p);
    ctx.beginPath();
    ctx.arc(m.x, m.y, 4, 0, Math.PI * 2);
    ctx.fill();
  });

  ctx.strokeStyle = "blue";
  predicted.forEach(p => {
    const m = map(p);
    ctx.beginPath();
    ctx.arc(m.x, m.y, 4, 0, Math.PI * 2);
    ctx.stroke();
  });
}
