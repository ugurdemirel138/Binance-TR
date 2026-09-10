const form = document.getElementById("config-form");
const startBtn = document.getElementById("start-btn");
const stopBtn = document.getElementById("stop-btn");
const formError = document.getElementById("form-error");

const statusDot = document.getElementById("status-dot");
const statusText = document.getElementById("status-text");
const symbolTag = document.getElementById("symbol-tag");
const pnlValue = document.getElementById("pnl-value");
const gridLadder = document.getElementById("grid-ladder");
const ordersBody = document.getElementById("orders-body");
const tradesBody = document.getElementById("trades-body");
const logOutput = document.getElementById("log-output");

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  formError.textContent = "";

  const payload = {
    symbol: document.getElementById("symbol").value.trim(),
    lower: document.getElementById("lower").value,
    upper: document.getElementById("upper").value,
    grid_count: document.getElementById("grid_count").value,
    investment: document.getElementById("investment").value,
  };

  if (!payload.symbol || !payload.lower || !payload.upper || !payload.investment) {
    formError.textContent = "Tüm alanları doldurun.";
    return;
  }
  if (parseFloat(payload.lower) >= parseFloat(payload.upper)) {
    formError.textContent = "Alt sınır, üst sınırdan küçük olmalı.";
    return;
  }

  startBtn.disabled = true;
  try {
    const res = await fetch("/api/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!data.ok) {
      formError.textContent = data.error || "Bot başlatılamadı.";
      startBtn.disabled = false;
    } else {
      stopBtn.disabled = false;
    }
  } catch (err) {
    formError.textContent = "Sunucuya bağlanılamadı.";
    startBtn.disabled = false;
  }
});

stopBtn.addEventListener("click", async () => {
  stopBtn.disabled = true;
  await fetch("/api/stop", { method: "POST" });
  startBtn.disabled = false;
});

function renderLadder(levels, orders) {
  if (!levels || levels.length === 0) {
    gridLadder.innerHTML = '<p class="empty-hint">Bot başlatıldığında seviyeler burada görünecek.</p>';
    return;
  }
  const orderByLevel = {};
  orders.forEach((o) => (orderByLevel[o.level] = o));

  gridLadder.innerHTML = levels
    .map((price, idx) => {
      const order = orderByLevel[idx];
      const cls = order ? (order.side === "BUY" ? "buy" : "sell") : "";
      const sideLabel = order ? order.side : "—";
      return `<div class="grid-level ${cls}">
        <span class="lvl-price">${price.toFixed(4)}</span>
        <span class="lvl-bar"></span>
        <span class="lvl-side">${sideLabel}</span>
      </div>`;
    })
    .join("");
}

function renderOrders(orders) {
  ordersBody.innerHTML = orders
    .map(
      (o) => `<tr>
        <td>${o.level}</td>
        <td class="side-${o.side.toLowerCase()}">${o.side}</td>
        <td>${o.price}</td>
        <td>${o.qty}</td>
      </tr>`
    )
    .join("");
}

function renderTrades(trades) {
  tradesBody.innerHTML = trades
    .slice()
    .reverse()
    .map(
      (t) => `<tr>
        <td>${t.time}</td>
        <td class="side-buy">${t.buy_price}</td>
        <td class="side-sell">${t.sell_price}</td>
        <td>${t.profit}</td>
      </tr>`
    )
    .join("");
}

async function poll() {
  try {
    const res = await fetch("/api/status");
    const s = await res.json();

    statusDot.classList.toggle("running", s.active);
    statusText.textContent = s.active ? "Bot çalışıyor" : "Bot durdu";
    symbolTag.textContent = s.symbol || "";
    pnlValue.textContent = (s.total_profit ?? 0).toFixed(6);

    startBtn.disabled = s.active;
    stopBtn.disabled = !s.active;

    renderLadder(s.levels, s.orders);
    renderOrders(s.orders);
    renderTrades(s.trades);
    logOutput.textContent = (s.logs || []).join("\n");
    logOutput.scrollTop = logOutput.scrollHeight;
  } catch (err) {
    // sunucu henüz ayakta değilse sessizce geç
  }
}

poll();
setInterval(poll, 3000);
