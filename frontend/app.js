const csvInput = document.getElementById("csv-file");
const scenarioSelect = document.getElementById("scenario");
const spacingInput = document.getElementById("station-spacing");
const spacingSection = document.getElementById("spacing-section");

const xSelect = document.getElementById("x-column");
const ySelect = document.getElementById("y-column");
const valueSelect = document.getElementById("value-column");

const createJobBtn = document.getElementById("create-job-btn");
const jobStatusEl = document.getElementById("job-status");

const resultActions = document.getElementById("result-actions");
const downloadBtn = document.getElementById("download-btn");
const plotBtn = document.getElementById("plot-btn");

const placeholder = document.getElementById("canvas-placeholder");
const plotContainer = document.getElementById("plot-container");

let currentJobId = null;
let pollInterval = null;

/* ================= CSV HEADER PARSING ================= */

csvInput.addEventListener("change", () => {
    const file = csvInput.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = e => {
        const firstLine = e.target.result.split("\n")[0];
        const headers = firstLine.split(",").map(h => h.trim());
        populateDropdowns(headers);
    };
    reader.readAsText(file);
});

function populateDropdowns(headers) {
    [xSelect, ySelect, valueSelect].forEach(select => {
        select.innerHTML = "";
        headers.forEach(h => {
            const opt = document.createElement("option");
            opt.value = h;
            opt.textContent = h;
            select.appendChild(opt);
        });
    });
}

/* ================= SCENARIO LOGIC ================= */

scenarioSelect.addEventListener("change", () => {
    if (scenarioSelect.value === "sparse") {
        spacingInput.disabled = true;
        spacingInput.value = "";
        spacingSection.style.opacity = "0.4";
    } else {
        spacingInput.disabled = false;
        spacingSection.style.opacity = "1";
    }
});

/* ================= CREATE JOB ================= */

createJobBtn.addEventListener("click", async () => {
    if (!csvInput.files.length) {
        alert("Upload a CSV file");
        return;
    }

    const formData = new FormData();
    formData.append("csv_file", csvInput.files[0]);
    formData.append("scenario", scenarioSelect.value);
    formData.append("x_column", xSelect.value);
    formData.append("y_column", ySelect.value);
    formData.append("value_column", valueSelect.value);

    if (!spacingInput.disabled && spacingInput.value) {
        formData.append("station_spacing", spacingInput.value);
    }

    jobStatusEl.textContent = "Running...";
    resultActions.classList.add("hidden");
    clearPlot();

    const res = await fetch("/jobs", { method: "POST", body: formData });
    const data = await res.json();

    currentJobId = data.job_id;
    startPolling();
});

/* ================= POLLING ================= */

function startPolling() {
    stopPolling();
    pollInterval = setInterval(async () => {
        const res = await fetch(`/jobs/${currentJobId}/status`);
        const data = await res.json();
        jobStatusEl.textContent = data.status.toUpperCase();

        if (data.status === "completed") {
            stopPolling();
            resultActions.classList.remove("hidden");
        }
    }, 2000);
}

function stopPolling() {
    if (pollInterval) clearInterval(pollInterval);
}

/* ================= DOWNLOAD ================= */

downloadBtn.onclick = () => {
    window.location.href = `/jobs/${currentJobId}/result.csv`;
};

/* ================= PLOT ================= */

plotBtn.onclick = async () => {
    const res = await fetch(`/jobs/${currentJobId}/result.json`);
    const rows = await res.json();

    const measured = rows.filter(r => r.source === "measured");
    const predicted = rows.filter(r => r.source === "predicted");

    placeholder.classList.add("hidden");
    plotContainer.classList.remove("hidden");

    Plotly.newPlot(plotContainer, [
        {
            x: measured.map(r => r.distance_along),
            y: measured.map(r => r.magnetic_value),
            mode: "markers",
            name: "Measured",
            marker: { color: "#3b82f6", size: 8 }
        },
        {
            x: predicted.map(r => r.distance_along),
            y: predicted.map(r => r.magnetic_value),
            mode: "markers",
            name: "Predicted",
            marker: {
                color: "#3b82f6",
                size: 8,
                symbol: "circle-open",
                line: { width: 2 }
            }
        }
    ], {
        paper_bgcolor: "#0e1117",
        plot_bgcolor: "#0e1117",
        font: { color: "#e5e7eb" },
        xaxis: { title: "Distance along traverse", gridcolor: "#1f2937" },
        yaxis: { title: "Magnetic value", gridcolor: "#1f2937" }
    });
};

function clearPlot() {
    Plotly.purge(plotContainer);
    plotContainer.classList.add("hidden");
    placeholder.classList.remove("hidden");
}
