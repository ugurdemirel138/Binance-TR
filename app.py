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
from datetime import datetime
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


bot = GridBot()


@app.route("/")
def index():
    return render_template("index.html", testnet=False)


@app.route("/api/start", methods=["POST"])
def api_start():
    data = request.get_json(force=True)
    try:
        bot.start(
            symbol=data["symbol"],
            lower=float(data["lower"]),
            upper=float(data["upper"]),
            grid_count=int(data["grid_count"]),
            investment=float(data["investment"]),
        )
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/stop", methods=["POST"])
def api_stop():
    bot.stop()
    return jsonify({"ok": True})


@app.route("/api/status")
def api_status():
    return jsonify(bot.status())


if __name__ == "__main__":
    if not API_KEY or not API_SECRET:
        print("UYARI: BINANCE_TR_API_KEY / BINANCE_TR_API_SECRET tanımlı değil.")
    app.run(debug=True, port=5000)
