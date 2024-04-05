var globalconfig = JSON.parse(
  document.getElementById("globalconfig").textContent,
);

function toggleFragment(id) {
  // check if we have to build anything
  const container = document.getElementById(id);
  const fragment = container.querySelector(".fragment-table");

  if (fragment.hasChildNodes()) {
    // fragment has data so no need to do anything
    return;
  }

  const data = globalconfig.fragmentProfiles[id];

  renderTable(fragment, data.info.fields, data.info.data);
}

function toggleFragmentMetrics(id) {
  // check if we have to build anything
  const container = document.getElementById(id);
  const metrics = container.querySelector(".metrics-table");

  if (metrics.hasChildNodes()) {
    // has data so no need to do anything
    return;
  }

  const data = globalconfig.fragmentProfiles[id].metrics;

  renderTable(metrics, data.fields, data.data);
}

function toggleOperator(id) {
  // check if we have to build anything
  const container = document.getElementById(id);
  const operator = container.querySelector(".operator-table");

  if (operator.hasChildNodes()) {
    // operator has data so no need to do anything
    return;
  }

  const data = globalconfig.operatorProfiles[id];

  renderTable(operator, data.info.fields, data.info.data);
}

function toggleOperatorMetrics(id) {
  // check if we have to build anything
  const container = document.getElementById(id);
  const metrics = container.querySelector(".metrics-table");

  if (metrics.hasChildNodes()) {
    // has data so no need to do anything
    return;
  }

  const data = globalconfig.operatorProfiles[id].metrics;

  renderTable(metrics, data.fields, data.data);
}

function toggleOperatorDetails(id) {
  // check if we have to build anything
  const container = document.getElementById(id);
  const details = container.querySelector(".details-table");

  if (details.hasChildNodes()) {
    // has data so no need to do anything
    return;
  }

  const data = globalconfig.operatorProfiles[id].details;
  renderTable(details, data.fields, data.data);
}

function toggleHostMetrics(id) {
  // check if we have to build anything
  const container = document.getElementById(id);
  const metrics = container.querySelector(".hostMetrics-table");

  if (metrics.hasChildNodes()) {
    // has data so no need to do anything
    return;
  }

  const data = globalconfig.operatorProfiles[id].hostMetrics;
  renderTable(metrics, data.fields, data.data);
}

function renderTable(container, fields, data) {
  // build the fragment table
  const table = document.createElement("table");
  table.className = "table text-right";

  // headers
  const thead = document.createElement("thead");
  const tr = document.createElement("tr");
  fields.forEach((field) => {
    const th = document.createElement("th");
    th.innerText = field;
    tr.appendChild(th);
  });
  thead.appendChild(tr);
  table.appendChild(thead);

  // body
  const tbody = document.createElement("tbody");
  data.forEach((cells) => {
    const tr = document.createElement("tr");
    cells.forEach((cell) => {
      const td = document.createElement("td");
      td.innerText = cell;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);

  container.appendChild(table);
}
