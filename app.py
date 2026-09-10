"""
Binance TR Grid Trading Bot — Web Arayüzlü
=============================================
Binance TR'nin RESMİ API'sini (www.binance.tr) kullanır.
Bu API, global Binance'tan tamamen ayrıdır:
- Sembol formatı: BTC_USDT (alt çizgili)
- side: 0=BUY, 1=SELL (string değil, sayı)
- type: 1=LIMIT
- İmzalama: HMAC SHA256, header: X-MBX-APIKEY

Grid mantığı, global Binance versiyonuyla birebir aynı:
fiyatın altına BUY emirleri açılır, dolunca bir üst seviyeye SELL
emri konur, o da dolunca kâr kaydedilip tekrar BUY açılır.
"""

import os
import math
import time
import hmac
import hashlib
import json
import threading
from datetime import datetime, timedelta
from urllib.parse import urlencode, urlparse

import requests
import websocket
from flask import Flask, jsonify, request, render_template
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

API_KEY = os.getenv("BINANCE_TR_API_KEY", "")
API_SECRET = os.getenv("BINANCE_TR_API_SECRET", "")

TRADE_BASE = "https://www.binance.tr"      # imzalı/trading endpoint'leri
MARKET_BASE = "https://api.binance.me"     # public piyasa verisi
WS_API_BASE = "wss://ws-api.binance.tr:443/ws-api/v3"  # kullanıcı veri akışı (WebSocket)

# --- QuotaGuard sabit IP proxy desteği ---
QUOTAGUARD_URL = os.getenv("QUOTAGUARDSTATIC_URL", "")


def _parse_proxy():
    if not QUOTAGUARD_URL:
        return None
    url = QUOTAGUARD_URL.split(",")[0].strip()
    parsed = urlparse(url)
    return {
        "url": url,
        "host": parsed.hostname,
        "port": parsed.port,
        "user": parsed.username,
        "password": parsed.password,
    }


PROXY = _parse_proxy()
REQUESTS_PROXIES = {"http": PROXY["url"], "https": PROXY["url"]} if PROXY else None

MAX_LOG_LINES = 200

STATUS_NEW = 0
STATUS_PARTIAL = 1
STATUS_FILLED = 2
STATUS_CANCELED = 3


def round_step(value: float, step: float) -> float:
    if step == 0:
        return value
    precision = int(round(-math.log10(step))) if step < 1 else 0
    return math.floor(value / step) * step if precision == 0 else round(
        math.floor(value / step) * step, precision
    )


def format_amount(value: float, step: float) -> str:
    """Binance'ın kabul etmediği bilimsel gösterimi (örn. 5.3e-05) önler."""
    decimals = max(0, int(round(-math.log10(step)))) if step and step < 1 else 0
    return f"{value:.{decimals}f}"


# Otomatik modda denenecek adaylar (likit TRY pariteleri, en başta en çok işlem göreni)
AUTO_CANDIDATE_SYMBOLS = [
    "BTC_TRY", "ETH_TRY", "BNB_TRY", "XRP_TRY", "SOL_TRY",
    "DOGE_TRY", "ADA_TRY", "AVAX_TRY", "TRX_TRY", "DOT_TRY",
]


def fetch_ticker_ws(symbol_flat: str, timeout: float = 8) -> dict:
    """miniTicker akışından güncel fiyat + 24 saatlik en yüksek/en düşük fiyatı alır."""
    result = {}
    done = threading.Event()
    stream_symbol = symbol_flat.lower()

    bases = [
        f"wss://stream-cloud.binance.tr/ws/{stream_symbol}@miniTicker",
        f"wss://stream-tr.2meta.app/ws/{stream_symbol}@miniTicker",
    ]

    for url in bases:
        result.clear()
        done.clear()

        def on_message(ws, message):
            try:
                data = json.loads(message)
                if data.get("c") is not None:
                    result["price"] = float(data["c"])
                    result["high"] = float(data.get("h", data["c"]))
                    result["low"] = float(data.get("l", data["c"]))
                    result["open"] = float(data.get("o", data["c"]))
                    result["quote_volume"] = float(data.get("q", 0))
            except Exception:
                pass
            done.set()
            try:
                ws.close()
            except Exception:
                pass

        def on_error(ws, error):
            done.set()

        ws_app = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error)
        run_kwargs = {}
        if PROXY:
            run_kwargs = {
                "http_proxy_host": PROXY["host"],
                "http_proxy_port": PROXY["port"],
                "http_proxy_auth": (PROXY["user"], PROXY["password"]),
                "proxy_type": "http",
            }
        t = threading.Thread(target=ws_app.run_forever, kwargs=run_kwargs, daemon=True)
        t.start()
        done.wait(timeout)
        if "price" in result:
            return result
    return {}


def fetch_multi_ticker(symbol_flats: list, timeout: float = 12) -> dict:
    """Birden fazla sembolün 24s verisini TEK WebSocket bağlantısından toplu çeker."""
    result = {}
    done = threading.Event()
    streams = "/".join(f"{s.lower()}@miniTicker" for s in symbol_flats)
    bases = [
        f"wss://stream-cloud.binance.tr/stream?streams={streams}",
        f"wss://stream-tr.2meta.app/stream?streams={streams}",
    ]
    target_count = len(symbol_flats)

    for url in bases:
        result.clear()
        done.clear()

        def on_message(ws, message):
            try:
                msg = json.loads(message)
                d = msg.get("data", msg)
                sym = d.get("s")
                if sym and d.get("c") is not None:
                    result[sym] = {
                        "price": float(d["c"]),
                        "open": float(d.get("o", d["c"])),
                        "quote_volume": float(d.get("q", 0)),
                    }
                if len(result) >= target_count:
                    done.set()
                    try:
                        ws.close()
                    except Exception:
                        pass
            except Exception:
                pass

        def on_error(ws, error):
            done.set()

        ws_app = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error)
        run_kwargs = {}
        if PROXY:
            run_kwargs = {
                "http_proxy_host": PROXY["host"],
                "http_proxy_port": PROXY["port"],
                "http_proxy_auth": (PROXY["user"], PROXY["password"]),
                "proxy_type": "http",
            }
        t = threading.Thread(target=ws_app.run_forever, kwargs=run_kwargs, daemon=True)
        t.start()
        done.wait(timeout)
        if result:
            return result
    return {}


def parse_kline_closes(raw) -> list:
    closes = []
    for item in raw:
        try:
            if isinstance(item, list) and len(item) >= 5:
                closes.append(float(item[4]))
            elif isinstance(item, dict):
                c = item.get("close") or item.get("c")
                if c is not None:
                    closes.append(float(c))
        except Exception:
            continue
    return closes


def fetch_klines(client, symbol: str, symbol_flat: str, interval="1h", limit=50, log_fn=None) -> list:
    attempts = [
        (MARKET_BASE, "/api/v3/klines", {"symbol": symbol_flat, "interval": interval, "limit": limit}),
        (TRADE_BASE, "/open/v1/market/klines", {"symbol": symbol, "interval": interval, "limit": limit}),
        (TRADE_BASE, "/open/v1/market/klines", {"symbol": symbol_flat, "interval": interval, "limit": limit}),
    ]
    for base, path, params in attempts:
        try:
            data = client.public_request(base, path, params)
            raw = data if isinstance(data, list) else data.get("data", [])
            if isinstance(raw, dict):
                raw = raw.get("list", [])
            closes = parse_kline_closes(raw)
            if closes:
                return closes
            elif log_fn:
                log_fn(f"Kline boş ({base}{path})")
        except Exception as e:
            if log_fn:
                log_fn(f"Kline hata ({base}{path}): {e}")
            continue
    return []


def compute_ema(values: list, period: int):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for price in values[period:]:
        ema = price * k + ema * (1 - k)
    return ema


def compute_rsi(values: list, period: int = 14):
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, period + 1):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for i in range(period + 1, len(values)):
        change = values[i] - values[i - 1]
        avg_gain = (avg_gain * (period - 1) + max(change, 0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-change, 0)) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def fetch_try_symbol_universe(client, limit=40) -> list:
    """Binance TR'deki tüm _TRY paritelerini (ve tiplerini) getirir."""
    data = client.public_request(TRADE_BASE, "/open/v1/common/symbols")
    symbols = data.get("data", {}).get("list", [])
    result = []
    for s in symbols:
        sym = s.get("symbol", "")
        if sym.endswith("_TRY"):
            result.append((sym, s.get("type", 1)))
    return result[:limit]


def get_account_balances(client) -> dict:
    """Varlık -> kullanılabilir (free) bakiye sözlüğü döner."""
    try:
        data = client.signed_request("GET", "/open/v1/account/spot", {})
        balances = data.get("data", {}).get("balances", [])
        return {b["asset"]: float(b.get("free", 0)) for b in balances}
    except Exception:
        return {}


def get_symbol_step(client, symbol: str):
    try:
        data = client.public_request(TRADE_BASE, "/open/v1/common/symbols")
        symbols = data.get("data", {}).get("list", [])
        info = next((s for s in symbols if s["symbol"] == symbol), None)
        if not info:
            return None
        for f in info["filters"]:
            if f["filterType"] == "LOT_SIZE":
                return float(f["stepSize"])
    except Exception:
        return None
    return None


def ensure_try_balance(client, needed_try: float, log_fn=None) -> bool:
    """TRY bakiyesi yetersizse, mevcut USDT'yi otomatik olarak TRY'ye çevirir."""
    balances = get_account_balances(client)
    try_balance = balances.get("TRY", 0)
    if try_balance >= needed_try:
        return True

    usdt_balance = balances.get("USDT", 0)
    if usdt_balance <= 0:
        if log_fn:
            log_fn(f"TRY bakiyesi yetersiz ({try_balance}) ve çevrilecek USDT bulunamadı.")
        return False

    if log_fn:
        log_fn(f"TRY bakiyesi yetersiz ({try_balance}). {usdt_balance} USDT, TRY'ye çevriliyor...")

    step = get_symbol_step(client, "USDT_TRY") or 0.01
    qty_str = format_amount(round_step(usdt_balance, step), step)
    try:
        client.signed_request(
            "POST", "/open/v1/orders",
            {"symbol": "USDT_TRY", "side": 1, "type": 2, "quantity": qty_str},
        )
        time.sleep(3)
        if log_fn:
            log_fn("USDT → TRY dönüşümü gönderildi.")
        return True
    except Exception as e:
        if log_fn:
            log_fn(f"HATA (USDT→TRY dönüşümü): {e}")
        return False


MAX_SMART_POSITIONS = 5
MIN_QUOTE_VOLUME_TRY = 50000
ACTIVITY_THRESHOLD_PCT = 1.5
SCAN_INTERVAL_SECONDS = 60  # 1 dakika


class SmartTrader:
    """Coin seçimini ve alım-satım kararını kendisi veren otonom mod."""

    def __init__(self):
        self.active = False
        self.thread = None
        self.lock = threading.Lock()
        self.total_investment = 0.0
        self.positions = {}
        self.realized_profit = 0.0
        self.logs = []
        self.client = None

        # --- basit uyarlama (adaptasyon) durumu ---
        self.trade_history = []          # [{"symbol", "profit", "time"}]
        self.symbol_strikes = {}         # symbol -> art arda zarar sayısı
        self.blacklist = {}              # symbol -> kara listeden çıkış zamanı
        self.activity_threshold = ACTIVITY_THRESHOLD_PCT
        self.rsi_band = [30, 70]

    def log(self, message: str):
        ts = datetime.now().strftime("%H:%M:%S")
        with self.lock:
            self.logs.append(f"[{ts}] {message}")
            if len(self.logs) > MAX_LOG_LINES:
                self.logs = self.logs[-MAX_LOG_LINES:]

    def start(self, total_investment: float):
        if self.active:
            raise RuntimeError("Akıllı otonom bot zaten çalışıyor.")
        self.client = BinanceTRClient(API_KEY, API_SECRET)
        self.total_investment = total_investment
        self.positions = {}
        self.realized_profit = 0.0
        self.logs = []
        self.trade_history = []
        self.symbol_strikes = {}
        self.blacklist = {}
        self.activity_threshold = ACTIVITY_THRESHOLD_PCT
        self.rsi_band = [30, 70]

        if not ensure_try_balance(self.client, total_investment, log_fn=self.log):
            raise RuntimeError(
                "TRY bakiyesi yetersiz ve otomatik dönüşüm başarısız oldu. "
                "Hesabında TRY veya USDT olduğundan emin ol."
            )

        self.active = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        self.log("Akıllı otonom bot başlatıldı.")

    def stop(self):
        if not self.active:
            return
        self.active = False
        self.log("Durduruluyor, açık pozisyonlar kapatılıyor...")
        for symbol in list(self.positions.keys()):
            self._sell_position(symbol, reason="Bot durduruldu")
        self.log("Akıllı otonom bot durduruldu.")

    def _get_symbol_filters(self, symbol):
        data = self.client.public_request(TRADE_BASE, "/open/v1/common/symbols")
        symbols = data.get("data", {}).get("list", [])
        info = next((s for s in symbols if s["symbol"] == symbol), None)
        if not info:
            return None
        tick = step = 0.0
        for f in info["filters"]:
            if f["filterType"] == "PRICE_FILTER":
                tick = float(f["tickSize"])
            if f["filterType"] == "LOT_SIZE":
                step = float(f["stepSize"])
        return {"type": info.get("type", 1), "tick": tick, "step": step}

    def _loop(self):
        while self.active:
            try:
                self._scan_and_trade()
                self._adapt()
            except Exception as e:
                self.log(f"HATA (tarama döngüsü): {e}")
            for _ in range(SCAN_INTERVAL_SECONDS):
                if not self.active:
                    break
                time.sleep(1)

    def _scan_and_trade(self):
        self.log("Piyasa taranıyor...")
        universe = fetch_try_symbol_universe(self.client)
        if not universe:
            self.log("Sembol listesi alınamadı, bu tur atlanıyor.")
            return

        symbol_flats = [s[0].replace("_", "") for s in universe]
        ticker_data = fetch_multi_ticker(symbol_flats)
        if not ticker_data:
            self.log("Piyasa verisi alınamadı, bu tur atlanıyor.")
            return

        scored = []
        for symbol, symbol_type in universe:
            flat = symbol.replace("_", "")
            info = ticker_data.get(flat)
            if not info or not info.get("open"):
                continue
            change_pct = abs((info["price"] - info["open"]) / info["open"] * 100)
            if info.get("quote_volume", 0) < MIN_QUOTE_VOLUME_TRY:
                continue
            scored.append({"symbol": symbol, "type": symbol_type,
                            "price": info["price"], "change_pct": change_pct})

        scored.sort(key=lambda x: x["change_pct"], reverse=True)
        now = datetime.now()
        scored = [s for s in scored if self.blacklist.get(s["symbol"], now) <= now]
        candidates = [s for s in scored if s["change_pct"] >= self.activity_threshold][:MAX_SMART_POSITIONS * 2]
        self.log(f"{len(candidates)} hareketli coin bulundu (eşik: %{self.activity_threshold:.2f}).")

        chosen_symbols = {c["symbol"] for c in candidates[:MAX_SMART_POSITIONS]}

        for symbol in list(self.positions.keys()):
            flat = symbol.replace("_", "")
            closes = fetch_klines(self.client, symbol, flat, interval="5m", log_fn=self.log)
            if symbol not in chosen_symbols or self._bearish(closes):
                self._sell_position(symbol, reason="sinyal/aralık dışı")

        slots = MAX_SMART_POSITIONS - len(self.positions)
        for c in candidates:
            if slots <= 0:
                break
            if c["symbol"] in self.positions:
                continue
            flat = c["symbol"].replace("_", "")
            closes = fetch_klines(self.client, c["symbol"], flat, interval="5m", log_fn=self.log)
            if self._bullish(closes):
                self._buy_position(c["symbol"], c["type"], c["price"])
                slots -= 1

    def _bullish(self, closes):
        if len(closes) < 21:
            return False
        ema9, ema21 = compute_ema(closes, 9), compute_ema(closes, 21)
        rsi = compute_rsi(closes, 14)
        low, high = self.rsi_band
        return bool(ema9 and ema21 and rsi and ema9 > ema21 and low < rsi < high)

    def _bearish(self, closes):
        if len(closes) < 21:
            return False
        ema9, ema21 = compute_ema(closes, 9), compute_ema(closes, 21)
        rsi = compute_rsi(closes, 14)
        return bool((ema9 and ema21 and ema9 < ema21) or (rsi and rsi > self.rsi_band[1] + 5))

    def _buy_position(self, symbol, symbol_type, price):
        filters = self._get_symbol_filters(symbol)
        if not filters:
            self.log(f"{symbol}: filtre bilgisi alınamadı, atlandı.")
            return
        budget = self.total_investment / MAX_SMART_POSITIONS
        qty = round_step(budget / price, filters["step"])
        if qty <= 0:
            self.log(f"{symbol}: hesaplanan miktar çok küçük, atlandı.")
            return
        qty_str = format_amount(qty, filters["step"])
        try:
            self.client.signed_request(
                "POST", "/open/v1/orders",
                {"symbol": symbol, "side": 0, "type": 2, "quantity": qty_str},
            )
            self.positions[symbol] = {
                "qty": qty, "entry_price": price, "symbol_type": symbol_type,
                "filters": filters, "time": datetime.now().strftime("%H:%M:%S"),
            }
            self.log(f"AL: {symbol} @ ~{price} (miktar {qty_str})")
        except Exception as e:
            self.log(f"HATA (alım {symbol}): {e}")

    def _sell_position(self, symbol, reason=""):
        pos = self.positions.get(symbol)
        if not pos:
            return
        qty_str = format_amount(pos["qty"], pos["filters"]["step"])
        try:
            self.client.signed_request(
                "POST", "/open/v1/orders",
                {"symbol": symbol, "side": 1, "type": 2, "quantity": qty_str},
            )
            sell_price = pos["entry_price"]
            info = fetch_ticker_ws(symbol.replace("_", ""), timeout=4)
            if info.get("price"):
                sell_price = info["price"]
            profit = (sell_price - pos["entry_price"]) * pos["qty"]
            self.realized_profit += profit
            self.trade_history.append({
                "symbol": symbol, "profit": profit,
                "time": datetime.now().strftime("%H:%M:%S"),
            })
            if profit < 0:
                self.symbol_strikes[symbol] = self.symbol_strikes.get(symbol, 0) + 1
                if self.symbol_strikes[symbol] >= 2:
                    self.blacklist[symbol] = datetime.now() + timedelta(hours=6)
                    self.log(f"{symbol} art arda zarar etti, 6 saat kara listeye alındı.")
            else:
                self.symbol_strikes[symbol] = 0
            self.log(f"SAT: {symbol} @ ~{sell_price} ({reason}), kâr/zarar: {round(profit, 2)}")
        except Exception as e:
            self.log(f"HATA (satım {symbol}): {e}")
        finally:
            del self.positions[symbol]

    def _adapt(self):
        """Son işlemlere bakıp eşikleri hafifçe ayarlar (basit, şeffaf kural)."""
        recent = self.trade_history[-10:]
        if len(recent) < 5:
            return
        win_rate = sum(1 for t in recent if t["profit"] > 0) / len(recent)
        if win_rate < 0.4:
            self.activity_threshold = min(5.0, self.activity_threshold + 0.5)
            self.rsi_band = [35, 65]
            self.log(f"Kazanma oranı düşük (%{win_rate*100:.0f}), eşikler sıkılaştırıldı.")
        elif win_rate > 0.6:
            self.activity_threshold = max(1.0, self.activity_threshold - 0.25)
            self.rsi_band = [30, 70]
            self.log(f"Kazanma oranı iyi (%{win_rate*100:.0f}), eşikler hafifçe gevşetildi.")

    def status(self):
        with self.lock:
            recent = self.trade_history[-10:]
            win_rate = (
                round(100 * sum(1 for t in recent if t["profit"] > 0) / len(recent))
                if recent else None
            )
            return {
                "active": self.active,
                "total_investment": self.total_investment,
                "realized_profit": round(self.realized_profit, 4),
                "positions": [
                    {"symbol": s, "qty": p["qty"], "entry_price": p["entry_price"], "time": p["time"]}
                    for s, p in self.positions.items()
                ],
                "logs": self.logs[-100:],
                "win_rate": win_rate,
                "activity_threshold": round(self.activity_threshold, 2),
                "blacklist": [s for s, until in self.blacklist.items() if until > datetime.now()],
            }


smart_trader = SmartTrader()


class BinanceTRClient:
    """Binance TR API için imzalı/imzasız istek yardımcı sınıfı."""

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret

    def _sign(self, params: dict) -> str:
        query = urlencode(params)
        return hmac.new(
            self.api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()

    def signed_request(self, method: str, path: str, params: dict = None):
        params = dict(params or {})
        params["timestamp"] = int(time.time() * 1000)
        params.setdefault("recvWindow", 5000)
        params["signature"] = self._sign(params)
        url = f"{TRADE_BASE}{path}"
        headers = {"X-MBX-APIKEY": self.api_key}
        if method == "GET":
            resp = requests.get(url, params=params, headers=headers, timeout=10, proxies=REQUESTS_PROXIES)
        else:
            resp = requests.post(url, params=params, headers=headers, timeout=10, proxies=REQUESTS_PROXIES)
        data = resp.json()
        if data.get("code") not in (0, None):
            raise RuntimeError(data.get("msg", "Bilinmeyen API hatası"))
        return data

    def public_request(self, base: str, path: str, params: dict = None):
        resp = requests.get(f"{base}{path}", params=params or {}, timeout=10, proxies=REQUESTS_PROXIES)
        return resp.json()


class GridBot:
    def __init__(self):
        self.client = None
        self.active = False
        self.thread = None
        self.lock = threading.Lock()

        self.symbol = None          # BTC_USDT (trading çağrıları için)
        self.symbol_flat = None     # BTCUSDT (piyasa verisi için)
        self.symbol_type = 1
        self.levels = []
        self.qty_per_grid = 0.0
        self.tick_size = 0.0
        self.step_size = 0.0

        self.orders = {}
        self.order_id_to_level = {}
        self.trades = []
        self.logs = []
        self.error = None

        self.ws = None
        self.ws_thread = None
        self.renew_thread = None

    def log(self, message: str):
        ts = datetime.now().strftime("%H:%M:%S")
        with self.lock:
            self.logs.append(f"[{ts}] {message}")
            if len(self.logs) > MAX_LOG_LINES:
                self.logs = self.logs[-MAX_LOG_LINES:]

    def _load_symbol_filters(self):
        data = self.client.public_request(TRADE_BASE, "/open/v1/common/symbols")
        symbols = data.get("data", {}).get("list", [])
        info = next((s for s in symbols if s["symbol"] == self.symbol), None)
        if info is None:
            raise ValueError(f"Sembol bulunamadı: {self.symbol}")
        self.symbol_type = info.get("type", 1)
        for f in info["filters"]:
            if f["filterType"] == "PRICE_FILTER":
                self.tick_size = float(f["tickSize"])
            if f["filterType"] == "LOT_SIZE":
                self.step_size = float(f["stepSize"])

    def _get_price_via_ws_ticker(self, timeout=8):
        """Ticker stream'inden tek bir fiyat okuması alır (REST trades boş dönerse)."""
        result = {}
        done = threading.Event()
        stream_symbol = self.symbol_flat.lower()

        bases = [
            f"wss://stream-cloud.binance.tr/ws/{stream_symbol}@miniTicker",
            f"wss://stream-tr.2meta.app/ws/{stream_symbol}@miniTicker",
        ]

        for url in bases:
            result.clear()
            done.clear()

            def on_message(ws, message, _url=url):
                try:
                    data = json.loads(message)
                    price = data.get("c") or data.get("close")
                    if price is not None:
                        result["price"] = float(price)
                        self.log(f"Fiyat kaynağı (WS ticker): {_url}")
                except Exception:
                    pass
                done.set()
                try:
                    ws.close()
                except Exception:
                    pass

            def on_error(ws, error):
                done.set()

            ws_app = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error)
            run_kwargs = {}
            if PROXY:
                run_kwargs = {
                    "http_proxy_host": PROXY["host"],
                    "http_proxy_port": PROXY["port"],
                    "http_proxy_auth": (PROXY["user"], PROXY["password"]),
                    "proxy_type": "http",
                }
            t = threading.Thread(target=ws_app.run_forever, kwargs=run_kwargs, daemon=True)
            t.start()
            done.wait(timeout)
            if "price" in result:
                return result["price"]
        return None

    def _get_current_price(self) -> float:
        ws_price = self._get_price_via_ws_ticker()
        if ws_price:
            return ws_price

        attempts = [
            (TRADE_BASE, "/open/v1/market/trades", {"symbol": self.symbol}),
            (TRADE_BASE, "/open/v1/market/trades", {"symbol": self.symbol_flat}),
            (MARKET_BASE, "/api/v3/trades", {"symbol": self.symbol_flat}),
        ]
        last_error = None
        for base, path, params in attempts:
            try:
                data = self.client.public_request(base, path, {**params, "limit": 1})
                raw = data if isinstance(data, list) else data.get("data", [])
                if isinstance(raw, dict):
                    raw = raw.get("list", [])
                trades = raw
                if trades:
                    self.log(f"Fiyat kaynağı: {base}{path}")
                    return float(trades[-1]["price"])
                else:
                    self.log(f"Boş cevap ({base}{path}): {str(data)[:200]}")
            except Exception as e:
                last_error = e
                self.log(f"HATA ({base}{path}): {e}")
                continue
        raise RuntimeError(
            f"Güncel fiyat hiçbir uç noktadan alınamadı (son hata: {last_error})."
        )

    def _round_price(self, price: float) -> float:
        return round_step(price, self.tick_size)

    def _round_qty(self, qty: float) -> float:
        return round_step(qty, self.step_size)

    def start(self, symbol, lower, upper, grid_count, investment):
        if self.active:
            raise RuntimeError("Bot zaten çalışıyor. Önce durdurun.")

        symbol = symbol.upper().strip()
        if "_" not in symbol:
            raise ValueError(
                "Binance TR sembolleri alt çizgili olmalı, örn: BTC_USDT"
            )

        self.client = BinanceTRClient(API_KEY, API_SECRET)
        self.symbol = symbol
        self.symbol_flat = symbol.replace("_", "")
        self.orders = {}
        self.trades = []
        self.logs = []
        self.error = None

        self._load_symbol_filters()

        step = (upper - lower) / grid_count
        self.levels = [round(lower + i * step, 8) for i in range(grid_count + 1)]

        current_price = self._get_current_price()
        self.log(f"{self.symbol} güncel fiyat: {current_price}")

        raw_qty = investment / grid_count / current_price
        self.qty_per_grid = self._round_qty(raw_qty)
        if self.qty_per_grid <= 0:
            raise ValueError(
                "Hesaplanan işlem miktarı çok küçük. Yatırım miktarını artırın."
            )

        self.active = True

        placed = 0
        for idx, level_price in enumerate(self.levels):
            if level_price < current_price:
                self._place_order(idx, "BUY")
                placed += 1

        if placed == 0:
            self.log(
                "Uyarı: mevcut fiyat girilen aralığın altında/dışında kaldığı "
                "için hiç BUY emri açılamadı. Aralığı kontrol edin."
            )

        self.thread = threading.Thread(target=self._start_user_stream, daemon=True)
        self.thread.start()
        self.renew_thread = threading.Thread(target=self._renew_loop, daemon=True)
        self.renew_thread.start()
        self.log("Grid bot başlatıldı (WebSocket ile izleniyor).")

    def _place_order(self, level_idx, side):
        price = self._round_price(self.levels[level_idx])
        qty = self.qty_per_grid
        price_str = format_amount(price, self.tick_size)
        qty_str = format_amount(qty, self.step_size)
        side_code = 0 if side == "BUY" else 1
        try:
            resp = self.client.signed_request(
                "POST",
                "/open/v1/orders",
                {
                    "symbol": self.symbol,
                    "side": side_code,
                    "type": 1,  # LIMIT
                    "quantity": qty_str,
                    "price": price_str,
                    "timeInForce": 1,  # GTC
                },
            )
            order_id = resp["data"]["orderId"]
            self.orders[level_idx] = {
                "orderId": order_id,
                "side": side,
                "price": price,
                "qty": qty,
            }
            self.order_id_to_level[str(order_id)] = level_idx
            self.log(f"{side} emri açıldı: seviye {level_idx} @ {price}")
        except Exception as e:
            self.error = str(e)
            self.log(f"HATA ({side} @ {price}): {e}")

    def _create_listen_token(self):
        resp = self.client.signed_request("POST", "/open/v1/user-listen-token", {})
        return resp["data"]["token"], resp["data"].get("expirationTime")

    def _start_user_stream(self):
        try:
            token, _ = self._create_listen_token()
        except Exception as e:
            self.log(f"HATA (listen token alınamadı): {e}")
            return

        def on_open(ws):
            ws.send(json.dumps({
                "id": "sub1",
                "method": "userDataStream.subscribe.listenToken",
                "params": {"listenToken": token},
            }))
            self.log("WebSocket bağlandı, kullanıcı verisine abone olundu.")

        def on_message(ws, message):
            try:
                msg = json.loads(message)
            except Exception:
                return
            event = msg.get("event")
            if not isinstance(event, dict):
                return
            e_type = event.get("e")
            if e_type == "executionReport" and event.get("X") == "FILLED":
                order_id = str(event.get("i"))
                level_idx = self.order_id_to_level.get(order_id)
                if level_idx is not None and level_idx in self.orders:
                    self._handle_fill(level_idx, self.orders[level_idx])
            elif e_type == "eventStreamTerminated":
                self.log("WebSocket oturumu sona erdi, yeniden bağlanılıyor.")
                if self.active:
                    threading.Thread(target=self._start_user_stream, daemon=True).start()

        def on_error(ws, error):
            self.log(f"WebSocket hatası: {error}")

        def on_close(ws, code, reason):
            self.log("WebSocket bağlantısı kapandı.")
            if self.active:
                time.sleep(5)
                threading.Thread(target=self._start_user_stream, daemon=True).start()

        self.ws = websocket.WebSocketApp(
            WS_API_BASE,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        run_kwargs = {"ping_interval": 180}
        if PROXY:
            run_kwargs.update({
                "http_proxy_host": PROXY["host"],
                "http_proxy_port": PROXY["port"],
                "http_proxy_auth": (PROXY["user"], PROXY["password"]),
                "proxy_type": "http",
            })
        self.ws.run_forever(**run_kwargs)

    def _renew_loop(self):
        # listenToken 24 saatte bir yenilenmeli; her 12 saatte bir yeniden bağlan.
        while self.active:
            time.sleep(12 * 3600)
            if self.active and self.ws:
                self.log("Token yenileniyor, WebSocket yeniden başlatılıyor.")
                try:
                    self.ws.close()
                except Exception:
                    pass

    def _handle_fill(self, level_idx, info):
        del self.orders[level_idx]
        side = info["side"]
        price = info["price"]
        qty = info["qty"]
        self.log(f"{side} emri doldu: seviye {level_idx} @ {price}")

        if side == "BUY" and level_idx + 1 < len(self.levels):
            self._place_order(level_idx + 1, "SELL")
        elif side == "SELL" and level_idx - 1 >= 0:
            profit = (price - self.levels[level_idx - 1]) * qty
            self.trades.append(
                {
                    "buy_price": self.levels[level_idx - 1],
                    "sell_price": price,
                    "qty": qty,
                    "profit": round(profit, 8),
                    "time": datetime.now().strftime("%H:%M:%S"),
                }
            )
            self.log(f"Kâr kaydedildi: {round(profit, 4)}")
            self._place_order(level_idx - 1, "BUY")

    def stop(self):
        if not self.active:
            return
        self.active = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        for level_idx, info in list(self.orders.items()):
            try:
                self.client.signed_request(
                    "POST", "/open/v1/orders/cancel", {"orderId": info["orderId"]}
                )
                self.log(f"Emir iptal edildi: seviye {level_idx}")
            except Exception as e:
                self.log(f"HATA (iptal): {e}")
        self.orders = {}
        self.order_id_to_level = {}
        self.log("Grid bot durduruldu.")

    def status(self):
        with self.lock:
            total_profit = round(sum(t["profit"] for t in self.trades), 8)
            return {
                "active": self.active,
                "symbol": self.symbol,
                "levels": self.levels,
                "orders": [
                    {"level": idx, **info} for idx, info in sorted(self.orders.items())
                ],
                "trades": self.trades[-50:],
                "total_profit": total_profit,
                "logs": self.logs[-100:],
                "error": self.error,
            }


bots = {}          # symbol -> GridBot
bots_lock = threading.Lock()


@app.route("/")
def index():
    return render_template("index.html", testnet=False)


@app.route("/api/start", methods=["POST"])
def api_start():
    """Manuel mod: tek bir sembol için, kullanıcının verdiği aralıkla başlatır."""
    data = request.get_json(force=True)
    symbol = data["symbol"].upper().strip()
    with bots_lock:
        existing = bots.get(symbol)
        if existing and existing.active:
            return jsonify({"ok": False, "error": f"{symbol} için zaten çalışan bir bot var."}), 400
        new_bot = GridBot()
        bots[symbol] = new_bot
    try:
        new_bot.start(
            symbol=symbol,
            lower=float(data["lower"]),
            upper=float(data["upper"]),
            grid_count=int(data["grid_count"]),
            investment=float(data["investment"]),
        )
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/auto_start", methods=["POST"])
def api_auto_start():
    """Otomatik mod: bot, coin ve fiyat aralığını kendisi seçer."""
    data = request.get_json(force=True)
    total_investment = float(data["investment"])
    count = max(1, min(int(data.get("count", 3)), len(AUTO_CANDIDATE_SYMBOLS)))
    grid_count = int(data.get("grid_count", 10))

    with bots_lock:
        running = {s for s, b in bots.items() if b.active}
    candidates = [s for s in AUTO_CANDIDATE_SYMBOLS if s not in running]

    if not candidates:
        return jsonify({"ok": False, "error": "Şu an başlatılabilecek boşta coin yok."}), 400

    chosen = candidates[:count]
    per_coin_investment = total_investment / len(chosen)

    check_client = BinanceTRClient(API_KEY, API_SECRET)
    if not ensure_try_balance(check_client, total_investment):
        return jsonify({
            "ok": False,
            "error": "TRY bakiyesi yetersiz ve otomatik dönüşüm başarısız oldu.",
        }), 400

    started, errors = [], []
    for symbol in chosen:
        symbol_flat = symbol.replace("_", "")
        info = fetch_ticker_ws(symbol_flat)
        price = info.get("price")
        if not price:
            errors.append(f"{symbol}: güncel fiyat alınamadı, atlandı.")
            continue

        high, low = info.get("high", price), info.get("low", price)
        width = max(high - low, price * 0.02) if high > low else price * 0.04
        lower, upper = price - width, price + width

        new_bot = GridBot()
        with bots_lock:
            bots[symbol] = new_bot
        try:
            new_bot.start(symbol, lower, upper, grid_count, per_coin_investment)
            started.append(symbol)
        except Exception as e:
            errors.append(f"{symbol}: {e}")

    if not started:
        return jsonify({"ok": False, "error": "Hiçbir coin başlatılamadı.", "details": errors}), 400

    return jsonify({"ok": True, "started": started, "errors": errors})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    data = request.get_json(force=True) or {}
    symbol = (data.get("symbol") or "").upper().strip()
    with bots_lock:
        target = bots.get(symbol)
    if target:
        target.stop()
    return jsonify({"ok": True})


@app.route("/api/stop_all", methods=["POST"])
def api_stop_all():
    with bots_lock:
        items = list(bots.values())
    for b in items:
        b.stop()
    return jsonify({"ok": True})


@app.route("/api/status")
def api_status():
    with bots_lock:
        items = list(bots.items())
    return jsonify({"bots": [{"symbol": s, **b.status()} for s, b in items]})


@app.route("/api/smart/start", methods=["POST"])
def api_smart_start():
    data = request.get_json(force=True)
    try:
        smart_trader.start(float(data["investment"]))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/smart/stop", methods=["POST"])
def api_smart_stop():
    smart_trader.stop()
    return jsonify({"ok": True})


@app.route("/api/smart/status")
def api_smart_status():
    return jsonify(smart_trader.status())


if __name__ == "__main__":
    if not API_KEY or not API_SECRET:
        print("UYARI: BINANCE_TR_API_KEY / BINANCE_TR_API_SECRET tanımlı değil.")
    app.run(debug=True, port=5000)
