const autoForm = document.getElementById("auto-form");
const autoStartBtn = document.getElementById("auto-start-btn");
const autoFormError = document.getElementById("auto-form-error");

const configForm = document.getElementById("config-form");
const startBtn = document.getElementById("start-btn");
const formError = document.getElementById("form-error");

const stopAllBtn = document.getElementById("stop-all-btn");
const pnlValue = document.getElementById("pnl-value");
const botsList = document.getElementById("bots-list");

autoForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  autoFormError.textContent = "";

  const investment = document.getElementById("auto-investment").value;
  const count = document.getElementById("auto-count").value;

  if (!investment) {
    autoFormError.textContent = "Toplam yatırım miktarını girin.";
    return;
  }

  autoStartBtn.disabled = true;
  try {
    const res = await fetch("/api/auto_start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ investment, count }),
    });
    const data = await res.json();
    if (!data.ok) {
      autoFormError.textContent = data.error || "Otomatik başlatma başarısız.";
    } else if (data.errors && data.errors.length) {
      autoFormError.textContent =
        `Başlatıldı: ${data.started.join(", ")}. Atlanan: ${data.errors.join(" | ")}`;
    }
  } catch (err) {
    autoFormError.textContent = "Sunucuya bağlanılamadı.";
  } finally {
    autoStartBtn.disabled = false;
  }
});

configForm.addEventListener("submit", async (e) => {
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
    }
  } catch (err) {
    formError.textContent = "Sunucuya bağlanılamadı.";
  } finally {
    startBtn.disabled = false;
  }
});

stopAllBtn.addEventListener("click", async () => {
  stopAllBtn.disabled = true;
  await fetch("/api/stop_all", { method: "POST" });
  stopAllBtn.disabled = false;
});

async function stopBot(symbol) {
  await fetch("/api/stop", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ symbol }),
  });
}

function renderBots(bots) {
  if (!bots || bots.length === 0) {
    botsList.innerHTML = '<p class="empty-hint">Henüz çalışan bot yok. Sol taraftan başlat.</p>';
    return;
  }

  botsList.innerHTML = bots
    .map((b) => {
      const openOrders = (b.orders || []).length;
      const trades = (b.trades || []).length;
      const logs = (b.logs || []).slice(-4).join("\n");
      const statusLabel = b.active ? "Çalışıyor" : "Durdu";
      return `<div class="bot-card">
        <div class="bot-card-header">
          <span class="status-dot ${b.active ? "running" : ""}"></span>
          <span class="bot-symbol">${b.symbol}</span>
          <span class="bot-profit">+${(b.total_profit ?? 0).toFixed(4)}</span>
          <button class="bot-stop-btn" data-symbol="${b.symbol}">Durdur</button>
        </div>
        <div class="bot-meta">${statusLabel} · Açık emir: ${openOrders} · Tamamlanan işlem: ${trades}</div>
        <pre class="bot-log-mini">${logs}</pre>
      </div>`;
    })
    .join("");

  botsList.querySelectorAll(".bot-stop-btn").forEach((btn) => {
    btn.addEventListener("click", () => stopBot(btn.dataset.symbol));
  });
}

async function poll() {
  try {
    const res = await fetch("/api/status");
    const data = await res.json();
    const bots = data.bots || [];

    const totalProfit = bots.reduce((sum, b) => sum + (b.total_profit || 0), 0);
    pnlValue.textContent = totalProfit.toFixed(6);

    renderBots(bots);
  } catch (err) {
    // sunucu henüz ayakta değilse sessizce geç
  }
}

poll();
setInterval(poll, 3000);
