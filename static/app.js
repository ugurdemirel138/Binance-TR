const smartForm = document.getElementById("smart-form");
const smartStartBtn = document.getElementById("smart-start-btn");
const smartStopBtn = document.getElementById("smart-stop-btn");
const smartFormError = document.getElementById("smart-form-error");
const smartPnlValue = document.getElementById("smart-pnl-value");
const smartPositions = document.getElementById("smart-positions");
const smartLog = document.getElementById("smart-log");

smartForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  smartFormError.textContent = "";
  const investment = document.getElementById("smart-investment").value;
  smartStartBtn.disabled = true;
  try {
    const res = await fetch("/api/smart/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ investment }),
    });
    const data = await res.json();
    if (!data.ok) {
      smartFormError.textContent = data.error || "Başlatılamadı.";
      smartStartBtn.disabled = false;
    }
  } catch (err) {
    smartFormError.textContent = "Sunucuya bağlanılamadı.";
    smartStartBtn.disabled = false;
  }
});

smartStopBtn.addEventListener("click", async () => {
  smartStopBtn.disabled = true;
  await fetch("/api/smart/stop", { method: "POST" });
});

function renderSmartStatus(s) {
  smartStartBtn.disabled = s.active;
  smartStopBtn.disabled = !s.active;
  smartPnlValue.textContent = (s.realized_profit ?? 0).toFixed(4);

  const winRateText = s.win_rate != null ? `Kazanma oranı: %${s.win_rate} · ` : "";
  const thresholdText = s.activity_threshold != null ? `Eşik: %${s.activity_threshold}` : "";
  const blacklistText = s.blacklist && s.blacklist.length
    ? ` · Kara liste: ${s.blacklist.join(", ")}` : "";

  if (!s.positions || s.positions.length === 0) {
    smartPositions.innerHTML =
      `<p class="bot-meta">${winRateText}${thresholdText}${blacklistText}</p>` +
      (s.active
        ? '<p class="empty-hint">Pozisyon yok, uygun sinyal aranıyor...</p>'
        : '<p class="empty-hint">Otonom bot çalışmıyor.</p>');
  } else {
    smartPositions.innerHTML =
      `<p class="bot-meta">${winRateText}${thresholdText}${blacklistText}</p>` +
      s.positions
        .map(
          (p) => `<div class="bot-card">
            <div class="bot-card-header">
              <span class="status-dot running"></span>
              <span class="bot-symbol">${p.symbol}</span>
              <span class="bot-meta" style="margin-left:auto;">giriş: ${p.entry_price} · ${p.time}</span>
            </div>
          </div>`
        )
        .join("");
  }
  smartLog.textContent = (s.logs || []).join("\n");
  smartLog.scrollTop = smartLog.scrollHeight;
}

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

  try {
    const res2 = await fetch("/api/smart/status");
    const smartData = await res2.json();
    renderSmartStatus(smartData);
  } catch (err) {
    // sessizce geç
  }
}

poll();
setInterval(poll, 3000);
