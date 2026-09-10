# Binance TR Grid Trading Bot

Binance TR'nin resmi API'sini (www.binance.tr) kullanan, web arayüzlü bir
grid trading botu. **Not: Binance TR'de test/sahte para (testnet) seçeneği
yoktur — bu bot doğrudan gerçek hesabınla çalışır.**

## Önemli farklar (global Binance botuna göre)

- Sembolleri alt çizgili yazman gerekiyor: `BTC_USDT`, `ETH_TRY` gibi.
- Testnet yok, her işlem gerçek.
- `python-binance` gibi hazır bir kütüphane yok, bu kod ham HTTP
  istekleriyle Binance TR'nin belgelenmiş API'sine bağlanıyor.

## Kurulum

```bash
cd binance-tr-bot
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# .env dosyasını açıp BINANCE_TR_API_KEY / BINANCE_TR_API_SECRET doldurun
```

## API key alma

1. Tarayıcıdan **binance.tr**'ye giriş yapın (mobil uygulamada bu özellik yok).
2. Profil → **API Management** (veya arama ile "API" yazıp bulun).
3. Yeni key oluşturun.
4. **Sadece "Trade" iznini açın, "Withdraw" (para çekme) iznini asla açmayın.**

## Çalıştırma

```bash
python app.py
```

`http://localhost:5000` adresini açın.

## Kullanım

Global Binance botuyla aynı mantık: sembol, alt/üst fiyat sınırı, grid
sayısı ve toplam yatırım gir, "Botu Başlat"a bas. Fiyat aralığın altına
otomatik alım emirleri açılır; dolan her alım, bir üst seviyeye satım
emrine dönüşür; satım dolduğunda kâr kaydedilip tekrar alım açılır.

## Uyarılar

- Gerçek parayla çalışır, testnet yok — küçük miktarla başlamanı öneririm.
- Fiyat aralığın dışına çıkarsa (özellikle aşağı) zarar riski vardır.
- Withdraw izni kapalıysa, API key çalınsa/sızsa bile para hesaptan
  çekilemez — en fazla kötü bir alım-satım yaptırılabilir.
