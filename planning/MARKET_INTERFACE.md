# Market Data Interface Design

Unified Python interface for market data in FinAlly. The simulator and Massive API client are two implementations behind one abstract interface. All downstream code — SSE streaming, trade fills, portfolio valuation — is source-agnostic.

## Core Data Model

```python
from dataclasses import dataclass, field

@dataclass
class PriceUpdate:
    """A single price update for one ticker, as seen by the rest of the system."""
    ticker: str
    price: float
    previous_price: float       # Price from the previous update (not necessarily prev-day close)
    timestamp: float            # Unix seconds
    change: float               # price - previous_price (intra-session delta)
    direction: str              # "up", "down", or "flat"
    day_change_pct: float | None = None   # Day change % from prior close (Massive only; None for simulator)
```

`PriceUpdate` is the only type that leaves the market data layer. Everything downstream works with `PriceUpdate` objects or reads from `PriceCache`.

---

## Abstract Interface

```python
from abc import ABC, abstractmethod

class MarketDataSource(ABC):
    """Abstract interface for market data providers.

    Implementations write updates into a shared PriceCache.
    They do not return prices directly — callers read from the cache.
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop producing price updates and release resources."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. Removes it from the cache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of active tickers."""
```

---

## Price Cache

Shared in-memory store. Data sources write to it; the SSE streamer and trade endpoint read from it.

```python
import time
from threading import Lock

class PriceCache:
    """Thread-safe store of the latest PriceUpdate per ticker."""

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        day_change_pct: float | None = None,
    ) -> PriceUpdate:
        """Write a new price for a ticker. Returns the resulting PriceUpdate."""
        with self._lock:
            ts = timestamp or time.time()
            previous = self._prices.get(ticker)
            previous_price = previous.price if previous else price

            if price > previous_price:
                direction = "up"
            elif price < previous_price:
                direction = "down"
            else:
                direction = "flat"

            update = PriceUpdate(
                ticker=ticker,
                price=price,
                previous_price=previous_price,
                timestamp=ts,
                change=price - previous_price,
                direction=direction,
                day_change_pct=day_change_pct,
            )
            self._prices[ticker] = update
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)

    def tickers(self) -> list[str]:
        with self._lock:
            return list(self._prices.keys())
```

---

## Factory Function

Selects the implementation at startup based on environment. This is the only place that imports either implementation.

```python
import os

def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Return the appropriate MarketDataSource for the current environment."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        from .massive_client import MassiveDataSource
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)

    from .simulator import SimulatorDataSource
    return SimulatorDataSource(price_cache=price_cache)
```

---

## Massive Implementation

Polls the Massive v3 snapshot endpoint on a configurable interval. Uses `asyncio.to_thread` because the `massive` Python client is synchronous.

```python
import asyncio
import logging
from massive import RESTClient

logger = logging.getLogger(__name__)

class MassiveDataSource(MarketDataSource):

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,    # seconds; free tier: 15s, paid: 2–5s
    ) -> None:
        self._client = RESTClient(api_key=api_key)
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._tickers = list(tickers)
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def add_ticker(self, ticker: str) -> None:
        if ticker not in self._tickers:
            self._tickers.append(ticker)

    async def remove_ticker(self, ticker: str) -> None:
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Massive poll failed: %s", e)
            await asyncio.sleep(self._interval)

    async def _poll_once(self) -> None:
        if not self._tickers:
            return

        tickers = list(self._tickers)   # snapshot to avoid mutation during iteration
        snapshots = await asyncio.to_thread(
            lambda: list(self._client.list_universal_snapshots(
                ticker_any_of=tickers,
            ))
        )

        for snap in snapshots:
            if not snap.last_trade or not snap.last_trade.price:
                continue
            self._cache.update(
                ticker=snap.ticker,
                price=float(snap.last_trade.price),
                timestamp=snap.last_trade.sip_timestamp / 1e9 if snap.last_trade.sip_timestamp else None,
                day_change_pct=snap.session.change_percent if snap.session else None,
            )
```

---

## Simulator Implementation

Drives the `GBMSimulator` (see `MARKET_SIMULATOR.md`) in an async loop at 500ms cadence.

```python
import asyncio
from .simulator import GBMSimulator

class SimulatorDataSource(MarketDataSource):

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._tickers: list[str] = []
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._tickers = list(tickers)
        self._sim = GBMSimulator(tickers=self._tickers)
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def add_ticker(self, ticker: str) -> None:
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            if self._sim:
                self._sim.add_ticker(ticker)

    async def remove_ticker(self, ticker: str) -> None:
        self._tickers = [t for t in self._tickers if t != ticker]
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(self._interval)
```

---

## SSE Integration

The SSE endpoint polls `PriceCache.get_all()` and pushes batched updates to clients. A single event per cycle contains all tickers — simpler for the frontend to reconcile than per-ticker events.

```python
import json
import asyncio
from fastapi.responses import StreamingResponse

async def price_stream_generator(price_cache: PriceCache):
    """Async generator for the SSE price stream."""
    while True:
        prices = price_cache.get_all()
        if prices:
            payload = {
                ticker: {
                    "ticker": p.ticker,
                    "price": p.price,
                    "previous_price": p.previous_price,
                    "change": p.change,
                    "direction": p.direction,
                    "timestamp": p.timestamp,
                    "day_change_pct": p.day_change_pct,
                }
                for ticker, p in prices.items()
            }
            yield f"data: {json.dumps(payload)}\n\n"
        await asyncio.sleep(0.5)

# FastAPI route
from fastapi import Request

async def stream_prices(request: Request, price_cache: PriceCache):
    async def generate():
        async for event in price_stream_generator(price_cache):
            if await request.is_disconnected():
                break
            yield event

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
```

**SSE event shape** (one event every ~500ms, all tickers):
```json
data: {
  "AAPL": {"ticker": "AAPL", "price": 191.23, "previous_price": 191.10, "change": 0.13, "direction": "up", "timestamp": 1748304000.5, "day_change_pct": 0.87},
  "GOOGL": {"ticker": "GOOGL", "price": 174.55, "previous_price": 174.60, "change": -0.05, "direction": "down", "timestamp": 1748304000.5, "day_change_pct": -0.21}
}
```

---

## Application Lifecycle

```
1. Startup
   ├── Create PriceCache
   ├── create_market_data_source(price_cache)  → MassiveDataSource or SimulatorDataSource
   └── await source.start(initial_tickers_from_db)

2. Watchlist mutation (API handlers)
   ├── POST /api/watchlist  → await source.add_ticker(ticker)
   └── DELETE /api/watchlist/{ticker}  → await source.remove_ticker(ticker)

3. SSE stream (GET /api/stream/prices)
   └── Reads price_cache.get_all() every 500ms

4. Trade execution (POST /api/portfolio/trade)
   └── Reads price_cache.get(ticker) for fill price

5. Shutdown (lifespan context manager)
   └── await source.stop()
```

---

## File Structure

```
backend/
  app/
    market/
      __init__.py         # Exports: PriceCache, PriceUpdate, create_market_data_source
      models.py           # PriceUpdate dataclass
      cache.py            # PriceCache class
      interface.py        # MarketDataSource ABC
      factory.py          # create_market_data_source()
      massive_client.py   # MassiveDataSource
      simulator.py        # SimulatorDataSource + GBMSimulator
      seed_prices.py      # SEED_PRICES, TICKER_PARAMS constants
```
