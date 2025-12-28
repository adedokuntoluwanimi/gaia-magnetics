const csvFileInput = document.getElementById("csvFile");
const scenarioSelect = document.getElementById("scenario");
const spacingInput = document.getElementById("spacing");

const xColumnSelect = document.getElementById("xColumn");
const yColumnSelect = document.getElementById("yColumn");
const valueColumnSelect = document.getElementById("valueColumn");

const createJobBtn = document.getElementById("createJobBtn");
const jobStatus = document.getElementById("jobStatus");

csvFileInput.addEventListener("change", handleCSV);
scenarioSelect.addEventListener("change", handleScenarioChange);
createJobBtn.addEventListener("click", createJob);

/* -----------------------------
   Scenario handling
----------------------------- */
function handleScenarioChange() {
  if (scenarioSelect.value === "explicit_geometry") {
    spacingInput.disabled = true;
    spacingInput.value = "";
  } else {
    spacingInput.disabled = false;
  }
}

function normalizeScenario(value) {
  if (value === "sparse_only") return "sparse";
  if (value === "explicit_geometry") return "explicit";
  return value;
}

/* -----------------------------
   CSV header extraction
----------------------------- */
function handleCSV(e) {
  const file = e.target.files[0];
  if (!file) return;

  const reader = new FileReader();
  reader.onload = () => {
    const firstLine = reader.result.split("\n")[0];
    const headers = firstLine.split(",").map(h => h.trim());

    populateSelect(xColumnSelect, headers);
    populateSelect(yColumnSelect, headers);
    populateSelect(valueColumnSelect, headers);
  };
  reader.readAsText(file);
}

function populateSelect(select, options) {
  select.innerHTML = "";
  options.forEach(opt => {
    const option = document.createElement("option");
    option.value = opt;
    option.textContent = opt;
    select.appendChild(option);
  });
}

/* -----------------------------
   Create job
----------------------------- */
async function createJob() {
  const file = csvFileInput.files[0];
  if (!file) {
    alert("Please upload a CSV file");
    return;
  }

  const form = new FormData();
  form.append("file", file);
  form.append("scenario", normalizeScenario(scenarioSelect.value));
  form.append("x_column", xColumnSelect.value);
  form.append("y_column", yColumnSelect.value);
  form.append("tmi_column", valueColumnSelect.value);

  if (scenarioSelect.value === "sparse_only") {
    form.append("station_spacing", spacingInput.value);
  }

  jobStatus.textContent = "Creating job...";

  try {
    const res = await fetch("/jobs", {
      method: "POST",
      body: form
    });

    const data = await res.json();
    jobStatus.textContent = JSON.stringify(data, null, 2);
  } catch (err) {
    jobStatus.textContent = "Error creating job";
  }
}
