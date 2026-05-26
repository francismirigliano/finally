# Massive API Reference (formerly Polygon.io)

Reference documentation for the Massive (formerly Polygon.io) REST API as used in FinAlly. Polygon.io rebranded as Massive.com on October 30, 2025; existing API keys and integrations continue to work unchanged.

## Overview

- **Base URL**: `https://api.massive.com` (legacy `https://api.polygon.io` still redirects)
- **Python package**: `massive` — install via `pip install -U massive` or `uv add massive`
- **Replaces**: `polygon-api-client` (same team, same API, new name)
- **Min Python version**: 3.9+
- **Current SDK version**: v2.8.0 (May 2026)
- **Auth**: API key via `MASSIVE_API_KEY` env var, or passed directly to `RESTClient`

## Authentication

```python
from massive import RESTClient

# Reads MASSIVE_API_KEY from environment automatically (recommended)
client = RESTClient()

# Or pass explicitly
client = RESTClient(api_key="your_key_here")
```

The client sends `Authorization: Bearer <API_KEY>` on every request.

## Rate Limits

| Plan | Limit | Recommended poll interval |
|------|-------|--------------------------|
| Free (Starter/Developer) | 5 requests/minute | 15 seconds |
| Paid (Advanced/Business) | Unlimited (stay under 100 req/s) | 2–5 seconds |

Free tier data is 15-minute delayed. Real-time data requires Advanced or Business plan.

---

## Endpoints Used in FinAlly

### 1. Universal Snapshot — Multiple Tickers (Primary Endpoint)

The modern v3 endpoint. Retrieves current price data for up to 250 tickers in a single API call. This is the main endpoint used for polling in FinAlly.

**REST**: `GET /v3/snapshot?ticker.any_of=AAPL,GOOGL,MSFT`

**Python client**:
```python
from massive import RESTClient

client = RESTClient()

snapshots = list(client.list_universal_snapshots(
    ticker_any_of=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
))

for snap in snapshots:
    print(f"{snap.ticker}: ${snap.last_trade.price}")
    print(f"  Day change: {snap.session.change_percent}%")
    print(f"  Market status: {snap.market_status}")   # "pre", "regular", "after", "closed"
```

**`UniversalSnapshot` field reference**:

| Field | Type | Description |
|-------|------|-------------|
| `ticker` | str | Ticker symbol |
| `type` | str | Asset type: `"stocks"`, `"options"`, `"fx"`, `"crypto"`, `"indices"` |
| `name` | str | Company name |
| `market_status` | str | `"pre"`, `"regular"`, `"after"`, `"closed"` |
| `last_trade.price` | float | Most recent trade price |
| `last_trade.size` | float | Trade size in shares |
| `last_trade.sip_timestamp` | int | Trade timestamp (nanoseconds) |
| `last_quote.ask_price` | float | Best ask |
| `last_quote.bid_price` | float | Best bid |
| `last_quote.ask_size` | float | Ask size |
| `last_quote.bid_size` | float | Bid size |
| `session.open` | float | Session open price |
| `session.high` | float | Session high |
| `session.low` | float | Session low |
| `session.close` | float | Session close (or latest price during session) |
| `session.volume` | float | Session volume |
| `session.change` | float | Change from previous close |
| `session.change_percent` | float | Percentage change from previous close |
| `last_minute.open` / `.close` | float | Most recent 1-minute bar OHLC |
| `fmv` | float | Fair market value (Business plan only) |

**Key fields for FinAlly**:
- `last_trade.price` — current price for display and trade fills
- `session.change_percent` — day change % for watchlist column
- `session.change` — absolute change for display
- `market_status` — used to show stale-data indicator in UI
- `last_trade.sip_timestamp` — convert from nanoseconds: `/ 1e9`

---

### 2. Full Market Snapshot — Legacy v2 (Still Supported)

The original v2 endpoint. Still functional but `list_universal_snapshots` is preferred for new code.

**REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL`

**Python client**:
```python
# get_snapshot_all is the v2 method — deprecated but functional
from massive.rest.models import SnapshotMarketType

snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT"],
)

for snap in snapshots:
    print(f"{snap.ticker}: ${snap.last_trade.price}")
    print(f"  Day change: {snap.todays_change_perc}%")
    print(f"  Prev day close: ${snap.prev_day.close}")
```

**v2 Ticker object fields** (camelCase in JSON, snake_case as Python attributes):

| JSON field | Python attribute | Description |
|------------|-----------------|-------------|
| `ticker` | `ticker` | Ticker symbol |
| `todaysChange` | `todays_change` | Absolute change from prev close |
| `todaysChangePerc` | `todays_change_perc` | Percentage change from prev close |
| `updated` | `updated` | Last update timestamp (Unix ms) |
| `day.open/high/low/close` | `day.open/high/low/close` | Today's OHLC |
| `day.volume` | `day.volume` | Today's volume |
| `prevDay.close` | `prev_day.close` | Previous day's closing price |
| `lastTrade.price` | `last_trade.price` | Most recent trade price |
| `lastTrade.size` | `last_trade.size` | Trade size |
| `lastTrade.timestamp` | `last_trade.timestamp` | Trade time (Unix ms) |
| `lastQuote.P` | `last_quote.ask` | Best ask price |
| `lastQuote.p` | `last_quote.bid` | Best bid price |

---

### 3. Single Ticker Snapshot

Detailed snapshot for one ticker. Use when a user clicks a ticker for the detail view.

```python
# v3 (preferred)
snaps = list(client.list_universal_snapshots(ticker_any_of=["AAPL"]))
snap = snaps[0] if snaps else None

# v2 alternative
from massive.rest.models import SnapshotMarketType
snap = client.get_snapshot_ticker(
    market_type=SnapshotMarketType.STOCKS,
    ticker="AAPL",
)
print(f"Price: ${snap.last_trade.price}")
print(f"Bid/Ask spread: ${snap.last_quote.bid_price} / ${snap.last_quote.ask_price}")
```

---

### 4. Previous Day Bar

Previous day's OHLC. Useful for seeding initial prices and reference close for day change calculation.

**REST**: `GET /v2/aggs/ticker/{ticker}/prev`

```python
prev = client.get_previous_close_agg(ticker="AAPL", adjusted=True)

# Returns a single object (not an iterator)
print(f"Previous close: ${prev.close}")
print(f"OHLC: O={prev.open} H={prev.high} L={prev.low} C={prev.close}")
print(f"Volume: {prev.volume}")
```

**`PreviousCloseAgg` fields**: `open`, `high`, `low`, `close`, `volume`, `vwap`, `timestamp`

---

### 5. Historical Aggregates (Bars)

Historical OHLCV bars. Not used for live polling, but needed for the main chart's historical view.

**REST**: `GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}`

```python
# 1-minute bars for the last trading day
aggs = []
for bar in client.list_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="minute",
    from_="2026-05-23",
    to="2026-05-23",
    adjusted=True,
    sort="asc",
    limit=50000,
):
    aggs.append(bar)

# Daily bars for 6 months
for bar in client.list_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="day",
    from_="2025-11-01",
    to="2026-05-01",
):
    print(f"{bar.timestamp}: O={bar.open} H={bar.high} L={bar.low} C={bar.close} V={bar.volume}")
```

**`Agg` fields**: `open`, `high`, `low`, `close`, `volume`, `vwap`, `timestamp` (Unix ms), `transactions`, `otc`

**`timespan` values**: `"second"`, `"minute"`, `"hour"`, `"day"`, `"week"`, `"month"`, `"quarter"`, `"year"`

---

### 6. Last Trade / Last Quote

Point-in-time lookups for a single ticker. Lower overhead than a full snapshot when you only need one ticker's price.

```python
# Most recent trade
trade = client.get_last_trade(ticker="AAPL")
print(f"Last trade: ${trade.price} x {trade.size} shares")
print(f"Exchange: {trade.exchange}")
print(f"Timestamp: {trade.sip_timestamp}")  # nanoseconds

# Most recent NBBO quote
quote = client.get_last_quote(ticker="AAPL")
print(f"Bid: ${quote.bid_price} x {quote.bid_size}")
print(f"Ask: ${quote.ask_price} x {quote.ask_size}")
```

---

## How FinAlly Uses the API

The Massive poller runs as an async background task:

1. Collects all tickers from the price cache's active set
2. Calls `list_universal_snapshots(ticker_any_of=tickers)` — one API call for all tickers
3. Extracts `last_trade.price`, `session.change_percent`, and `last_trade.sip_timestamp`
4. Writes updates to the shared `PriceCache`
5. Sleeps for the poll interval, then repeats

```python
import asyncio
from massive import RESTClient

async def poll_massive(api_key: str, get_tickers, price_cache, interval: float = 15.0):
    """Background task: poll Massive API and push updates into the price cache."""
    client = RESTClient(api_key=api_key)

    while True:
        tickers = get_tickers()
        if tickers:
            try:
                snapshots = await asyncio.to_thread(
                    lambda: list(client.list_universal_snapshots(
                        ticker_any_of=tickers,
                    ))
                )
                for snap in snapshots:
                    if snap.last_trade and snap.last_trade.price:
                        price_cache.update(
                            ticker=snap.ticker,
                            price=snap.last_trade.price,
                            day_change_pct=snap.session.change_percent if snap.session else None,
                            timestamp=snap.last_trade.sip_timestamp / 1e9 if snap.last_trade.sip_timestamp else None,
                        )
            except Exception as e:
                # Log and continue — a failed poll should not crash the stream
                pass

        await asyncio.sleep(interval)
```

---

## Error Handling

The client raises `requests.HTTPError` (or a Massive subclass) for HTTP errors:

| Status | Cause | Action |
|--------|-------|--------|
| 401 | Invalid API key | Log and surface configuration error |
| 403 | Plan doesn't include endpoint | Log — real-time data needs Advanced plan |
| 429 | Rate limit exceeded | Back off, increase poll interval |
| 5xx | Server error | Retry with exponential backoff (client retries 3x by default) |

---

## Notes

- `list_universal_snapshots()` uses the v3 `/v3/snapshot` endpoint with automatic pagination; wrapping in `list()` is safe for ≤250 tickers.
- Timestamps from `last_trade.sip_timestamp` are nanoseconds; divide by `1e9` for Unix seconds.
- During off-hours, `last_trade.price` is the last traded price (may be after-hours or prior session).
- The `session` object resets at market open; in pre-market, `session.change_percent` reflects change from the prior session's close.
- For FinAlly's purposes, always check `snap.last_trade is not None` before accessing price — thinly traded or newly listed tickers may have no recent trade.
- The `market_status` field on `UniversalSnapshot` should drive the UI's stale-data indicator: show a yellow dot when status is not `"regular"`.
