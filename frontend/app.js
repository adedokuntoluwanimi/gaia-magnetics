const csvFileInput = document.getElementById("csvFile");
const scenarioSelect = document.getElementById("scenario");
const spacingInput = document.getElementById("spacing");

const xColumnSelect = document.getElementById("xColumn");
const yColumnSelect = document.getElementById("yColumn");
const valueColumnSelect = document.getElementById("valueColumn");

const createJobBtn = document.getElementById("createJobBtn");
const downloadBtn = document.getElementById("downloadBtn");
const plotBtn = document.getElementById("plotBtn");
const jobStatus = document.getElementById("jobStatus");

const canvas = document.getElementById("plotCanvas");
const ctx = canvas.getContext("2d");

let currentJobId = null;

/* ---------------- Scenario ---------------- */
scenarioSelect.addEventListener("change", () => {
  spacingInput.disabled = scenarioSelect.value === "explicit_geometry";
});

/* ---------------- CSV headers ---------------- */
csvFileInput.addEventListener("change", () => {
  const file = csvFileInput.files[0];
  const reader = new FileReader();

  reader.onload = () => {
    const headers = reader.result.split("\n")[0].split(",").map(h => h.trim());
    populateSelect(xColumnSelect, headers);
    populateSelect(yColumnSelect, headers);
    populateSelect(valueColumnSelect, headers);
  };

  reader.readAsText(file);
});

function populateSelect(select, options) {
  select.innerHTML = "";
  options.forEach(opt => {
    const o = document.createElement("option");
    o.value = opt;
    o.textContent = opt;
    select.appendChild(o);
  });
}

function normalizeScenario(v) {
  return v === "sparse_only" ? "sparse" : "explicit";
}

/* ---------------- Create Job ---------------- */
createJobBtn.addEventListener("click", async () => {
  const file = csvFileInput.files[0];
  if (!file) {
    alert("Upload a CSV first");
    return;
  }

  const form = new FormData();
  form.append("file", file);
  form.append("scenario", normalizeScenario(scenarioSelect.value));
  form.append("x_col", xColumnSelect.value);
  form.append("y_col", yColumnSelect.value);
  form.append("tmi_col", valueColumnSelect.value);

  if (scenarioSelect.value === "sparse_only") {
    form.append("station_spacing", spacingInput.value);
  }

  jobStatus.textContent = "Running job...";

  const res = await fetch("/jobs", { method: "POST", body: form });
  const data = await res.json();

  currentJobId = data.job_id;
  jobStatus.textContent = JSON.stringify(data, null, 2);

  downloadBtn.disabled = false;
  plotBtn.disabled = false;
});

/* ---------------- Download ---------------- */
downloadBtn.addEventListener("click", () => {
  window.location.href = `/jobs/${currentJobId}/download`;
});

/* ---------------- Plot ---------------- */
plotBtn.addEventListener("click", async () => {
  const res = await fetch(`/jobs/${currentJobId}/download`);
  const text = await res.text();

  const rows = text.trim().split("\n");
  const header = rows.shift().split(",");

  const idx = {
    d: header.indexOf("d_along"),
    t: header.indexOf("tmi"),
    m: header.indexOf("is_measured")
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

function drawPlot(measured, predicted) {
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  const all = measured.concat(predicted);
  const xs = all.map(p => p.x);
  const ys = all.map(p => p.y);

  const pad = 60;
  const xmin = Math.min(...xs);
  const xmax = Math.max(...xs);
  const ymin = Math.min(...ys);
  const ymax = Math.max(...ys);

  const sx = (canvas.width - 2 * pad) / (xmax - xmin);
  const sy = (canvas.height - 2 * pad) / (ymax - ymin);

  const map = p => ({
    x: pad + (p.x - xmin) * sx,
    y: canvas.height - (pad + (p.y - ymin) * sy)
  });

  /* Axes */
  ctx.strokeStyle = "#aaa";
  ctx.beginPath();
  ctx.moveTo(pad, pad);
  ctx.lineTo(pad, canvas.height - pad);
  ctx.lineTo(canvas.width - pad, canvas.height - pad);
  ctx.stroke();

  ctx.fillText("Distance along traverse (m)", canvas.width / 2 - 80, canvas.height - 20);
  ctx.save();
  ctx.rotate(-Math.PI / 2);
  ctx.fillText("Magnetic value (TMI)", -canvas.height / 2 - 40, 20);
  ctx.restore();

  /* Measured */
  ctx.fillStyle = "blue";
  measured.forEach(p => {
    const m = map(p);
    ctx.beginPath();
    ctx.arc(m.x, m.y, 4, 0, Math.PI * 2);
    ctx.fill();
  });

  /* Predicted */
  ctx.strokeStyle = "blue";
  predicted.forEach(p => {
    const m = map(p);
    ctx.beginPath();
    ctx.arc(m.x, m.y, 4, 0, Math.PI * 2);
    ctx.stroke();
  });
}
