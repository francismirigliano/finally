# Market Data Backend — Implementation Design

Complete implementation guide for the FinAlly market data subsystem. Covers the unified interface, price cache, GBM simulator, Massive API client, SSE streaming endpoint, and FastAPI lifecycle integration. All code snippets are production-ready and incorporate lessons from the code review (`planning/archive/MARKET_DATA_REVIEW.md`).

Everything in this document lives under `backend/app/market/`.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model)
4. [Price Cache — `cache.py`](#4-price-cache)
5. [Abstract Interface — `interface.py`](#5-abstract-interface)
6. [Seed Prices & Parameters — `seed_prices.py`](#6-seed-prices--parameters)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client)
9. [Factory — `factory.py`](#9-factory)
10. [SSE Streaming — `stream.py`](#10-sse-streaming)
11. [Package `__init__.py`](#11-package-initpy)
12. [FastAPI Lifecycle Integration](#12-fastapi-lifecycle-integration)
13. [Watchlist Coordination](#13-watchlist-coordination)
14. [Testing Strategy](#14-testing-strategy)
15. [Error Handling Reference](#15-error-handling-reference)
16. [Configuration Summary](#16-configuration-summary)

---

## 1. Architecture Overview

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM price simulation (default, no API key required)
└── MassiveDataSource    →  Polygon.io REST poller (when MASSIVE_API_KEY is set)
        │
        ▼
   PriceCache (thread-safe, in-memory, single point of truth)
        │
        ├──→ SSE stream endpoint  (GET /api/stream/prices)  — pushes to browser
        ├──→ Trade execution      (POST /api/portfolio/trade) — fill at market price
        └──→ Portfolio valuation  (GET /api/portfolio)       — mark-to-market P&L
```

### Design principles

- **Strategy pattern** — simulator and Massive client implement the same ABC; all downstream code is source-agnostic.
- **Push-to-cache model** — data sources write into `PriceCache` on their own schedule; consumers read from it independently. No coupling between producer timing and consumer timing.
- **Single container, single port** — market data runs as an asyncio background task inside the FastAPI process; no separate service.
- **SSE over WebSockets** — one-way push is all we need; simpler, no bidirectional complexity, universal browser support with automatic reconnection.

---

## 2. File Structure

```
backend/
  app/
    market/
      __init__.py         # Re-exports public API
      models.py           # PriceUpdate dataclass
      cache.py            # PriceCache — thread-safe price store
      interface.py        # MarketDataSource ABC
      seed_prices.py      # Constants: seed prices, GBM params, correlation groups
      simulator.py        # GBMSimulator + SimulatorDataSource
      massive_client.py   # MassiveDataSource (Polygon.io REST)
      factory.py          # create_market_data_source() — environment-driven selection
      stream.py           # FastAPI SSE router
  tests/
    market/
      test_models.py
      test_cache.py
      test_simulator.py
      test_simulator_source.py
      test_factory.py
      test_massive.py
```

---

## 3. Data Model

**File: `backend/app/market/models.py`**

`PriceUpdate` is the only data structure that crosses the market data boundary. Every consumer — SSE streaming, trade execution, portfolio valuation — works exclusively with this type.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a ticker's price at a point in time.

    frozen=True: safe to share across async tasks and threads without copying.
    slots=True: minor memory optimization — many instances created per second.
    """

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from the previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from the previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' — computed from price vs previous_price."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission. Single serialization point."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Why computed properties instead of stored fields

`change`, `change_percent`, and `direction` are derived from `price` and `previous_price`. Storing them separately would risk stale/inconsistent values. Properties eliminate that risk entirely — they always reflect the actual prices.

---

## 4. Price Cache

**File: `backend/app/market/cache.py`**

The central data hub. One data source writes into it; SSE streaming, portfolio valuation, and trade execution read from it. Must be thread-safe because the Massive client runs API calls via `asyncio.to_thread()` (real OS thread) while SSE reads on the async event loop.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price per ticker.

    Writers: SimulatorDataSource or MassiveDataSource (exactly one active).
    Readers: SSE endpoint, portfolio valuation, trade execution.

    Uses threading.Lock (not asyncio.Lock) because the Massive client's
    synchronous REST calls run in asyncio.to_thread(), which operates in
    a real OS thread — asyncio.Lock would not protect against that.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Incremented on every write; used by SSE for change detection

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the resulting PriceUpdate.

        On the first update for a ticker, previous_price == price (direction='flat').
        Subsequent updates carry the previous update's price, so direction and change
        are intra-session deltas, not day-over-day changes.
        """
        with self._lock:
            ts = timestamp if timestamp is not None else time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Latest price for a single ticker, or None if not in cache."""
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: return just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow copy of all current prices. Safe for SSE serialization."""
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)
            self._version += 1

    @property
    def version(self) -> int:
        """Monotonically increasing write counter.

        CPython GIL makes reading a single int atomic, so no lock needed here.
        The SSE loop compares this against its last-sent version to skip
        redundant sends (important when Massive only updates every 15 seconds).
        """
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Version-based change detection in SSE

Without the version counter, the SSE loop would serialize and push all prices every 500ms even when nothing changed (e.g., Massive API only updates every 15 seconds — that would send 30 identical frames per API response). The version counter lets SSE skip sends efficiently:

```python
last_version = -1
while True:
    current_version = price_cache.version
    if current_version != last_version:
        last_version = current_version
        yield serialize_prices(price_cache.get_all())
    await asyncio.sleep(0.5)
```

---

## 5. Abstract Interface

**File: `backend/app/market/interface.py`**

The contract that both data sources implement. No business logic — just the lifecycle and ticker management API.

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache. This decouples the data source's update cadence
    from the SSE push cadence.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])   # once at startup
        # ... application runs ...
        await source.add_ticker("TSLA")               # on watchlist add
        await source.remove_ticker("GOOGL")           # on watchlist remove
        # ... shutdown ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts the background task and seeds the cache with initial prices.
        Must be called exactly once. Behavior if called twice is undefined.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Idempotent — safe to call multiple times or if start() was never called.
        After stop(), the source will not write to the cache again.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present.

        The next update cycle includes this ticker. For the simulator, the
        cache is seeded immediately so the ticker has a price right away.
        """

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. No-op if not present.

        Also removes the ticker from PriceCache.
        """

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Why the push-to-cache model

The data source writes to the cache; it does not return prices directly to callers. This push model means:

- The SSE endpoint reads on its own 500ms cadence regardless of whether the simulator ticked or Massive polled.
- Trade execution reads the latest cached price — always fast, never blocks on network.
- Multiple SSE clients all read from the same cache with no additional work per client.
- Swapping simulator for Massive (or vice versa) requires zero changes outside the factory.

---

## 6. Seed Prices & Parameters

**File: `backend/app/market/seed_prices.py`**

Constants only — no logic, no imports. Shared by the simulator and potentially as fallback prices when Massive hasn't responded yet.

```python
"""Seed prices and per-ticker GBM parameters for the market simulator."""

# Approximate real-world price ranges as of project creation.
# The simulator does not track actual prices — these are plausible starting points.
SEED_PRICES: dict[str, float] = {
    "AAPL":  190.00,
    "GOOGL": 175.00,
    "MSFT":  420.00,
    "AMZN":  185.00,
    "TSLA":  250.00,
    "NVDA":  800.00,
    "META":  500.00,
    "JPM":   195.00,
    "V":     280.00,
    "NFLX":  600.00,
}

# Per-ticker GBM parameters.
# sigma: annualized volatility — higher = more dramatic price movement per tick
# mu:    annualized drift — modest positive values prevent prices drifting toward zero
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},   # High vol — TSLA is its own thing
    "NVDA":  {"sigma": 0.40, "mu": 0.08},   # High vol, strong positive drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},   # Low vol — large-cap bank
    "V":     {"sigma": 0.17, "mu": 0.04},   # Low vol — payments network
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

# Fallback parameters for dynamically-added tickers not in the table above.
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Sector correlation groups for Cholesky decomposition.
# Tickers within the same group move more in sync.
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Pairwise correlation coefficients
INTRA_TECH_CORR    = 0.65   # Tech stocks correlate strongly during broad moves
INTRA_FINANCE_CORR = 0.55   # Finance stocks correlate with each other
TSLA_CORR          = 0.25   # TSLA moves independently; low correlation with all
CROSS_SECTOR_CORR  = 0.30   # Cross-sector or unknown-ticker fallback
```

### Volatility calibration

| Ticker | Sigma | Expected intraday range |
|--------|-------|------------------------|
| V | 0.17 | ~1.1% (calm, large-cap) |
| JPM | 0.18 | ~1.1% |
| AAPL | 0.22 | ~1.4% |
| GOOGL | 0.25 | ~1.6% |
| AMZN | 0.28 | ~1.8% |
| META | 0.30 | ~1.9% |
| NFLX | 0.35 | ~2.2% |
| NVDA | 0.40 | ~2.5% |
| TSLA | 0.50 | ~3.2% |

*Expected intraday range ≈ sigma × √(1/252). These match observed real-world ranges.*

---

## 7. GBM Simulator

**File: `backend/app/market/simulator.py`**

Two classes: `GBMSimulator` (pure math engine) and `SimulatorDataSource` (the `MarketDataSource` wrapper that drives the loop and writes to `PriceCache`).

### 7.1 The GBM Math

At each time step, a price evolves as:

```
S(t+dt) = S(t) × exp((μ - σ²/2) × dt + σ × √dt × Z)
```

Where:
- `S(t)` — current price
- `μ` (mu) — annualized drift (expected return)
- `σ` (sigma) — annualized volatility
- `dt` — time step as a fraction of a trading year
- `Z` — standard normal random variable

**Calibrating `dt` for 500ms updates:**
```
Trading year ≈ 252 days × 6.5 h/day × 3600 s/h = 5,896,800 seconds
dt = 0.5 / 5,896,800 ≈ 8.48 × 10⁻⁸
```

This tiny `dt` produces sub-cent moves per tick — realistic noise that accumulates into plausible intraday ranges over a simulated session. Prices are always positive because `exp(...)` is always positive.

### 7.2 Correlated Moves via Cholesky Decomposition

Real stocks do not move independently. We generate correlated draws using Cholesky decomposition of a sector-based correlation matrix.

Given correlation matrix `C` and its Cholesky factor `L = cholesky(C)`:
```
Z_correlated = L @ Z_independent
```

The resulting `Z_correlated` has the covariance structure of `C`. The matrix is recomputed when tickers are added or removed — O(n³) but n < 50, so negligible.

### 7.3 Implementation

```python
from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_SECTOR_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices.

    Math:
        S(t+dt) = S(t) * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)

    Correlated draws are generated via Cholesky decomposition of a
    sector-based correlation matrix: tech stocks correlate at 0.65,
    finance at 0.55, TSLA at 0.25 with everything, cross-sector at 0.30.
    """

    # 500ms expressed as a fraction of a trading year
    TRADING_SECONDS_PER_YEAR: float = 252 * 6.5 * 3600   # 5,896,800 seconds
    DEFAULT_DT: float = 0.5 / TRADING_SECONDS_PER_YEAR    # ≈ 8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._register_ticker(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers one time step. Returns {ticker: new_price}.

        This is the hot path — called every 500ms. Generates correlated
        GBM moves for all active tickers in a single numpy operation.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z_ind = np.random.standard_normal(n)
        z = self._cholesky @ z_ind if self._cholesky is not None else z_ind

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]

            drift = (mu - 0.5 * sigma ** 2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * float(z[i])
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random shock: ~0.1% chance per tick per ticker.
            # With 10 tickers at 2 ticks/sec, expect a visible shock ~every 50s.
            if random.random() < self._event_prob:
                magnitude = random.uniform(0.02, 0.05)
                direction = random.choice([-1, 1])
                self._prices[ticker] *= 1.0 + magnitude * direction
                logger.debug(
                    "Shock event: %s %+.1f%%",
                    ticker,
                    magnitude * 100 * direction,
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. No-op if already present."""
        if ticker in self._prices:
            return
        self._register_ticker(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. No-op if not present."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        """Current simulated price for a ticker, or None if not tracked."""
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        """Current list of tracked tickers."""
        return list(self._tickers)

    # --- Internals ---

    def _register_ticker(self, ticker: str) -> None:
        """Add ticker to internal state without rebuilding Cholesky.

        Called in bulk during __init__ — Cholesky is rebuilt once after all
        tickers are added, not once per ticker.
        """
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Recompute the Cholesky factor L where C = L @ L.T.

        With n <= 1, no correlation matrix is needed (single ticker moves are
        uncorrelated by definition). With n >= 2, build the n×n correlation
        matrix from sector assignments and decompose it.

        A small regularization term (1e-8 × I) is added to ensure the matrix
        is strictly positive definite even when dynamically-added tickers cause
        near-singular configurations.
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n, dtype=float)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho

        corr += np.eye(n) * 1e-8   # Regularize for numerical stability
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Correlation coefficient for a pair of tickers based on sector membership."""
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR            # TSLA is idiosyncratic
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR      # 0.65 within tech
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR   # 0.55 within finance
        return CROSS_SECTOR_CORR        # 0.30 for everything else


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by GBMSimulator.

    Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to PriceCache.

    Key behaviors:
    - Immediate cache seeding on start() and add_ticker() — the SSE endpoint
      has data to send before the first loop tick completes.
    - Exception resilience — a single bad step does not kill the data feed.
    - Clean cancellation — stop() cancels the task and awaits its completion.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(
            tickers=tickers,
            event_probability=self._event_prob,
        )
        # Seed cache immediately so SSE has prices to send on its first tick
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

        self._task = asyncio.create_task(self._run_loop(), name="gbm-simulator")
        logger.info("GBM simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("GBM simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

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
                logger.exception("Simulator step failed — continuing")
            await asyncio.sleep(self._interval)
```

---

## 8. Massive API Client

**File: `backend/app/market/massive_client.py`**

Polls the Massive (formerly Polygon.io) v3 snapshot endpoint for all watched tickers in a single API call. The synchronous `massive` client runs in `asyncio.to_thread()` to avoid blocking the event loop.

### Rate limits

| Plan | Limit | Poll interval |
|------|-------|---------------|
| Free (Starter/Developer) | 5 req/min | `15.0` seconds (default) |
| Paid (Advanced/Business) | Unlimited (<100 req/s) | `2.0–5.0` seconds |

Free tier data is 15-minute delayed. Real-time data requires an Advanced or Business plan.

```python
from __future__ import annotations

import asyncio
import logging

from massive import RESTClient

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Uses the v3 universal snapshot endpoint to fetch all watched tickers
    in a single API call. Runs in asyncio.to_thread() because the massive
    client is synchronous.

    Error handling:
    - Individual malformed snapshots are skipped; others are still processed.
    - Network/auth errors are logged and the loop retries on the next interval.
    - Cache retains last-known prices during outages (stale but not empty).
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._client = RESTClient(api_key=api_key)
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._tickers = list(tickers)

        # Immediate first poll so SSE has data before the first interval expires
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (next poll will include it)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Interval loop. The first poll runs in start() so we sleep first."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """One poll cycle: fetch v3 universal snapshots, write to cache."""
        if not self._tickers:
            return

        try:
            tickers = list(self._tickers)   # snapshot to avoid mutation during iteration
            snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)

            processed = 0
            for snap in snapshots:
                try:
                    if not snap.last_trade or snap.last_trade.price is None:
                        continue
                    price = float(snap.last_trade.price)
                    timestamp = (
                        snap.last_trade.sip_timestamp / 1e9
                        if snap.last_trade.sip_timestamp
                        else None
                    )
                    self._cache.update(
                        ticker=snap.ticker,
                        price=price,
                        timestamp=timestamp,
                    )
                    processed += 1
                except (AttributeError, TypeError, ValueError) as exc:
                    logger.warning(
                        "Skipping malformed snapshot for %s: %s",
                        getattr(snap, "ticker", "???"),
                        exc,
                    )

            logger.debug(
                "Massive poll: %d/%d tickers updated",
                processed,
                len(tickers),
            )

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Massive poll failed: %s", exc)
            # Do not re-raise — the loop retries on the next interval.
            # Common causes: 401 (bad key), 429 (rate limit), network timeout.

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Synchronous fetch — runs in asyncio.to_thread(). Returns snapshot list.

        Uses the v3 universal snapshot endpoint which handles up to 250 tickers
        in a single call and supports automatic pagination via list().
        """
        return list(self._client.list_universal_snapshots(
            ticker_any_of=tickers,
        ))
```

### Key fields used from the v3 snapshot

| Field | Usage |
|-------|-------|
| `snap.ticker` | Ticker symbol for cache key |
| `snap.last_trade.price` | Fill price for display and trade execution |
| `snap.last_trade.sip_timestamp` | Nanoseconds → divide by 1e9 for Unix seconds |
| `snap.market_status` | `"regular"` / `"pre"` / `"after"` / `"closed"` — for UI stale-data indicator |

### Timestamp handling

Massive v3 `last_trade.sip_timestamp` is in **nanoseconds**. Convert to Unix seconds:
```python
timestamp = snap.last_trade.sip_timestamp / 1e9
```

---

## 9. Factory

**File: `backend/app/market/factory.py`**

The single place that reads `MASSIVE_API_KEY` and selects an implementation. All other code remains source-agnostic.

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select and instantiate the appropriate MarketDataSource.

    Selection logic:
    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise                         → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        from .massive_client import MassiveDataSource
        logger.info("Market data: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)

    from .simulator import SimulatorDataSource
    logger.info("Market data: GBM Simulator (no MASSIVE_API_KEY set)")
    return SimulatorDataSource(price_cache=price_cache)
```

---

## 10. SSE Streaming

**File: `backend/app/market/stream.py`**

The `/api/stream/prices` endpoint holds open a long-lived HTTP connection and pushes price updates as `text/event-stream`. Clients use the native browser `EventSource` API which handles reconnection automatically.

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Factory: create the SSE router with an injected PriceCache reference.

    Called once at FastAPI startup. Using a factory avoids module-level
    state that would cause route double-registration in tests.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Pushes all tracked ticker prices every ~500ms. Each event contains
        the full price map so the frontend can reconcile any missed updates.

        The 'retry: 1000' directive tells EventSource to reconnect after
        1 second if the connection drops. The browser handles this natively.
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",   # Disable nginx/proxy buffering
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends all prices whenever the cache version changes. If nothing has
    changed (e.g., between Massive API polls), no event is sent — avoiding
    redundant network traffic.

    Stops cleanly when the client disconnects.
    """
    yield "retry: 1000\n\n"

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected from %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    payload = json.dumps({
                        ticker: update.to_dict()
                        for ticker, update in prices.items()
                    })
                    yield f"data: {payload}\n\n"

            await asyncio.sleep(interval)

    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for %s", client_ip)
```

### Wire format

Each SSE event contains the full price map for all tracked tickers:

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1748304000.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{"ticker":"GOOGL","price":175.12,"previous_price":175.20,"timestamp":1748304000.5,"change":-0.08,"change_percent":-0.046,"direction":"down"}}

data: {"AAPL":{"ticker":"AAPL","price":190.53,...},...}
```

### Frontend consumption

```javascript
const eventSource = new EventSource('/api/stream/prices');

eventSource.onmessage = (event) => {
    const prices = JSON.parse(event.data);
    // prices: { "AAPL": { ticker, price, previous_price, change, change_percent, direction, timestamp }, ... }
    for (const [ticker, data] of Object.entries(prices)) {
        updateWatchlistRow(ticker, data);   // flash green/red, update sparkline
    }
};

eventSource.onerror = () => {
    // EventSource retries automatically after 1000ms (per retry directive)
    setConnectionStatus('reconnecting');
};
```

### Why poll-and-push over event-driven push

The SSE generator polls the cache on a fixed 500ms interval rather than being notified by the data source on each write. This gives:

- **Regular cadence** — the frontend's sparkline chart fills in at predictable intervals, making the visualization smooth.
- **Batching** — a single SSE event contains all tickers; the frontend does one reconciliation pass per event.
- **Decoupling** — the SSE layer does not know or care whether updates came from the simulator (500ms) or Massive (15s); it sees only the cache.

---

## 11. Package `__init__.py`

**File: `backend/app/market/__init__.py`**

```python
"""Market data subsystem for FinAlly.

Public API — import from here, not from submodules:
    PriceUpdate               Immutable price snapshot dataclass
    PriceCache                Thread-safe in-memory price store
    MarketDataSource          Abstract interface for data providers
    create_market_data_source Factory: selects simulator or Massive based on env
    create_stream_router      FastAPI SSE router factory
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

---

## 12. FastAPI Lifecycle Integration

**File: `backend/app/main.py`** (relevant excerpts)

The market data system starts and stops with the FastAPI application via the `lifespan` context manager.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException

from app.market import PriceCache, MarketDataSource, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan: start market data on boot, stop it on shutdown."""

    # 1. Create the shared price cache
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    # 2. Select the appropriate data source (simulator or Massive)
    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # 3. Load the initial watchlist from SQLite and start the data source
    initial_tickers = await _load_watchlist_from_db()   # e.g., ["AAPL", "GOOGL", ...]
    await source.start(initial_tickers)

    # 4. Register the SSE streaming router (must happen after cache is ready)
    stream_router = create_stream_router(price_cache)
    app.include_router(stream_router)

    yield   # Application is running

    # 5. Clean shutdown: cancel background task
    await source.stop()


app = FastAPI(title="FinAlly API", lifespan=lifespan)


# --- FastAPI dependency functions ---

def get_price_cache() -> PriceCache:
    return app.state.price_cache


def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

### Injecting market data into route handlers

```python
from fastapi import APIRouter, Depends

router = APIRouter(prefix="/api")


@router.get("/portfolio")
async def get_portfolio(
    price_cache: PriceCache = Depends(get_price_cache),
):
    positions = await db.get_positions()
    for pos in positions:
        pos.current_price = price_cache.get_price(pos.ticker)   # fast, from cache
        pos.unrealized_pnl = (pos.current_price - pos.avg_cost) * pos.quantity
    return {"positions": positions, ...}


@router.post("/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    price = price_cache.get_price(trade.ticker)
    if price is None:
        raise HTTPException(
            status_code=400,
            detail=f"No price available for {trade.ticker}. "
                   f"Wait a moment and try again (Massive may not have polled yet).",
        )
    # ... execute trade at `price` ...


@router.post("/watchlist")
async def add_to_watchlist(
    body: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
):
    await db.insert_watchlist_entry(body.ticker)
    await source.add_ticker(body.ticker)   # simulator: immediate seed; Massive: next poll
    return {"ticker": body.ticker, "status": "added"}


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    await db.delete_watchlist_entry(ticker)
    # Only stop tracking if the user has no open position in this ticker
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)
    return {"ticker": ticker, "status": "removed"}
```

---

## 13. Watchlist Coordination

Whenever the watchlist changes (via REST API or LLM chat tool call), the market data source must be notified.

### Adding a ticker

```
POST /api/watchlist  {"ticker": "PYPL"}
  → INSERT INTO watchlist (user_id, ticker) VALUES ('default', 'PYPL')
  → await source.add_ticker("PYPL")
      Simulator:  GBMSimulator.add_ticker() → Cholesky rebuild → cache seeded immediately
      Massive:    append "PYPL" to _tickers list → appears in next poll cycle
  → Return 200 {"ticker": "PYPL", "price": <current or null>}
```

### Removing a ticker

```
DELETE /api/watchlist/PYPL
  → DELETE FROM watchlist WHERE user_id = 'default' AND ticker = 'PYPL'
  → Check positions table: does user hold shares of PYPL?
      No position (or quantity=0)  → await source.remove_ticker("PYPL")
      Has position                 → keep tracking (needed for portfolio P&L)
  → Return 200 {"ticker": "PYPL", "status": "removed"}
```

### Position-held guard

If a user removes a ticker from the watchlist while holding shares, prices must keep flowing for portfolio valuation. This guard prevents a stale-price scenario:

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str, ...):
    await db.delete_watchlist_entry(ticker)

    position = await db.get_position(ticker)
    if not position or position.quantity == 0:
        await source.remove_ticker(ticker)   # stops tracking & removes from cache
    # else: source keeps tracking; ticker stays in price cache for P&L
```

---

## 14. Testing Strategy

### 14.1 Unit tests — `PriceUpdate` model

**File: `backend/tests/market/test_models.py`**

```python
import time
from app.market.models import PriceUpdate


def test_direction_up():
    u = PriceUpdate(ticker="AAPL", price=191.0, previous_price=190.0, timestamp=time.time())
    assert u.direction == "up"
    assert u.change == 1.0
    assert u.change_percent > 0


def test_direction_down():
    u = PriceUpdate(ticker="AAPL", price=189.0, previous_price=190.0, timestamp=time.time())
    assert u.direction == "down"
    assert u.change == -1.0


def test_direction_flat():
    u = PriceUpdate(ticker="AAPL", price=190.0, previous_price=190.0, timestamp=time.time())
    assert u.direction == "flat"
    assert u.change == 0.0


def test_to_dict_keys():
    u = PriceUpdate(ticker="AAPL", price=190.0, previous_price=189.5, timestamp=1.0)
    d = u.to_dict()
    assert set(d.keys()) == {"ticker", "price", "previous_price", "timestamp",
                              "change", "change_percent", "direction"}


def test_immutable():
    u = PriceUpdate(ticker="AAPL", price=190.0, previous_price=189.0, timestamp=1.0)
    try:
        u.price = 200.0   # type: ignore[misc]
        assert False, "Should have raised FrozenInstanceError"
    except Exception:
        pass


def test_change_percent_zero_guard():
    u = PriceUpdate(ticker="X", price=100.0, previous_price=0.0, timestamp=1.0)
    assert u.change_percent == 0.0   # No division by zero
```

### 14.2 Unit tests — `PriceCache`

**File: `backend/tests/market/test_cache.py`**

```python
from app.market.cache import PriceCache


def test_update_and_get():
    cache = PriceCache()
    update = cache.update("AAPL", 190.50)
    assert update.ticker == "AAPL"
    assert update.price == 190.50
    assert cache.get("AAPL") == update


def test_first_update_direction_is_flat():
    cache = PriceCache()
    update = cache.update("AAPL", 190.50)
    assert update.direction == "flat"
    assert update.previous_price == update.price


def test_second_update_direction_up():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    update = cache.update("AAPL", 191.00)
    assert update.direction == "up"
    assert update.previous_price == 190.00


def test_second_update_direction_down():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    update = cache.update("AAPL", 189.00)
    assert update.direction == "down"


def test_get_unknown_returns_none():
    cache = PriceCache()
    assert cache.get("NOPE") is None
    assert cache.get_price("NOPE") is None


def test_get_price_convenience():
    cache = PriceCache()
    cache.update("AAPL", 190.50)
    assert cache.get_price("AAPL") == 190.50


def test_get_all_returns_copy():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    cache.update("GOOGL", 175.00)
    all_prices = cache.get_all()
    assert set(all_prices.keys()) == {"AAPL", "GOOGL"}
    all_prices["FAKE"] = None   # modifying the copy should not affect the cache
    assert "FAKE" not in cache.get_all()


def test_remove():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    cache.remove("AAPL")
    assert cache.get("AAPL") is None


def test_remove_nonexistent_is_noop():
    cache = PriceCache()
    cache.remove("NOPE")   # should not raise


def test_version_increments_on_update():
    cache = PriceCache()
    v0 = cache.version
    cache.update("AAPL", 190.00)
    assert cache.version == v0 + 1
    cache.update("AAPL", 191.00)
    assert cache.version == v0 + 2


def test_version_increments_on_remove():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    v = cache.version
    cache.remove("AAPL")
    assert cache.version == v + 1


def test_contains():
    cache = PriceCache()
    cache.update("AAPL", 190.00)
    assert "AAPL" in cache
    assert "NOPE" not in cache


def test_len():
    cache = PriceCache()
    assert len(cache) == 0
    cache.update("AAPL", 190.00)
    cache.update("GOOGL", 175.00)
    assert len(cache) == 2
```

### 14.3 Unit tests — `GBMSimulator`

**File: `backend/tests/market/test_simulator.py`**

```python
import numpy as np
import random

import pytest
from app.market.simulator import GBMSimulator
from app.market.seed_prices import SEED_PRICES


class TestGBMSimulator:

    def test_step_returns_all_tickers(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        result = sim.step()
        assert set(result.keys()) == {"AAPL", "GOOGL"}

    def test_prices_are_always_positive(self):
        sim = GBMSimulator(tickers=["AAPL", "TSLA"])
        for _ in range(5_000):
            prices = sim.step()
            assert all(p > 0 for p in prices.values())

    def test_initial_prices_match_seeds(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]

    def test_unknown_ticker_seed_price_in_range(self):
        sim = GBMSimulator(tickers=["ZZZZ"])
        price = sim.get_price("ZZZZ")
        assert price is not None
        assert 50.0 <= price <= 300.0

    def test_add_ticker(self):
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("TSLA")
        result = sim.step()
        assert "TSLA" in result

    def test_add_ticker_duplicate_is_noop(self):
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("AAPL")
        assert len(sim.get_tickers()) == 1

    def test_remove_ticker(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        sim.remove_ticker("GOOGL")
        result = sim.step()
        assert "GOOGL" not in result
        assert "AAPL" in result

    def test_remove_nonexistent_is_noop(self):
        sim = GBMSimulator(tickers=["AAPL"])
        sim.remove_ticker("NOPE")   # should not raise

    def test_empty_tickers_step(self):
        sim = GBMSimulator(tickers=[])
        assert sim.step() == {}

    def test_prices_drift_over_time(self):
        sim = GBMSimulator(tickers=["AAPL"])
        initial = sim.get_price("AAPL")
        for _ in range(1_000):
            sim.step()
        assert sim.get_price("AAPL") != initial

    def test_cholesky_none_for_single_ticker(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None

    def test_cholesky_exists_for_multiple_tickers(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        assert sim._cholesky is not None
        assert sim._cholesky.shape == (2, 2)

    def test_cholesky_rebuilds_on_add(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None
        sim.add_ticker("MSFT")
        assert sim._cholesky is not None

    def test_full_default_watchlist_cholesky(self):
        """Cholesky must succeed for the full 10-ticker default set."""
        tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
                   "NVDA", "META", "JPM", "V", "NFLX"]
        sim = GBMSimulator(tickers=tickers)
        assert sim._cholesky is not None
        assert sim._cholesky.shape == (10, 10)
        result = sim.step()
        assert len(result) == 10

    def test_shock_event_magnitude(self):
        """When a shock fires, price must move by 2-5%."""
        sim = GBMSimulator(tickers=["AAPL"], event_probability=1.0)  # force shocks
        original = sim.get_price("AAPL")
        prices = sim.step()
        # The GBM drift is tiny relative to a 2-5% shock
        pct_change = abs(prices["AAPL"] - original) / original
        assert 0.01 < pct_change < 0.10   # a bit of slack for drift + shock

    def test_get_tickers_public(self):
        sim = GBMSimulator(tickers=["AAPL", "MSFT"])
        assert set(sim.get_tickers()) == {"AAPL", "MSFT"}
```

### 14.4 Integration tests — `SimulatorDataSource`

**File: `backend/tests/market/test_simulator_source.py`**

```python
import asyncio
import pytest
from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource


@pytest.mark.asyncio
class TestSimulatorDataSource:

    async def test_start_seeds_cache_immediately(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10.0)
        await source.start(["AAPL", "GOOGL"])
        # Prices must be in cache before the first loop tick
        assert cache.get("AAPL") is not None
        assert cache.get("GOOGL") is not None
        await source.stop()

    async def test_cache_updates_over_time(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.05)
        await source.start(["AAPL"])
        v0 = cache.version
        await asyncio.sleep(0.25)   # allow several ticks
        assert cache.version > v0
        await source.stop()

    async def test_add_ticker_seeds_immediately(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10.0)
        await source.start(["AAPL"])
        await source.add_ticker("TSLA")
        assert cache.get("TSLA") is not None
        assert "TSLA" in source.get_tickers()
        await source.stop()

    async def test_remove_ticker_clears_cache(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10.0)
        await source.start(["AAPL", "TSLA"])
        await source.remove_ticker("TSLA")
        assert cache.get("TSLA") is None
        assert "TSLA" not in source.get_tickers()
        await source.stop()

    async def test_stop_is_idempotent(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])
        await source.stop()
        await source.stop()   # second stop must not raise
```

### 14.5 Unit tests — `MassiveDataSource` (mocked)

**File: `backend/tests/market/test_massive.py`**

```python
from unittest.mock import MagicMock, patch, AsyncMock
import pytest
from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource


def _make_snapshot(ticker: str, price: float, sip_timestamp_ns: int = 1_707_580_800_000_000_000) -> MagicMock:
    """Build a mock Massive v3 UniversalSnapshot."""
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade = MagicMock()
    snap.last_trade.price = price
    snap.last_trade.sip_timestamp = sip_timestamp_ns
    return snap


def _make_source(interval: float = 60.0) -> tuple[MassiveDataSource, PriceCache]:
    cache = PriceCache()
    source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=interval)
    return source, cache


@pytest.mark.asyncio
class TestMassiveDataSource:

    async def test_poll_once_updates_cache(self):
        source, cache = _make_source()
        snapshots = [_make_snapshot("AAPL", 190.50), _make_snapshot("GOOGL", 175.25)]
        source._tickers = ["AAPL", "GOOGL"]

        with patch.object(source, "_fetch_snapshots", return_value=snapshots):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("GOOGL") == 175.25

    async def test_timestamp_conversion(self):
        source, cache = _make_source()
        ns = 1_707_580_800_000_000_000   # 1707580800.0 Unix seconds
        source._tickers = ["AAPL"]
        snapshots = [_make_snapshot("AAPL", 190.50, sip_timestamp_ns=ns)]

        with patch.object(source, "_fetch_snapshots", return_value=snapshots):
            await source._poll_once()

        update = cache.get("AAPL")
        assert update is not None
        assert abs(update.timestamp - 1_707_580_800.0) < 1.0

    async def test_malformed_snapshot_skipped(self):
        source, cache = _make_source()
        source._tickers = ["AAPL", "BAD"]

        good = _make_snapshot("AAPL", 190.50)
        bad = MagicMock()
        bad.ticker = "BAD"
        bad.last_trade = None   # triggers AttributeError in poll_once

        with patch.object(source, "_fetch_snapshots", return_value=[good, bad]):
            await source._poll_once()   # must not raise

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("BAD") is None

    async def test_api_error_does_not_crash(self):
        source, cache = _make_source()
        source._tickers = ["AAPL"]

        with patch.object(source, "_fetch_snapshots", side_effect=Exception("connection refused")):
            await source._poll_once()   # must not raise

        assert cache.get_price("AAPL") is None

    async def test_add_ticker(self):
        source, cache = _make_source()
        source._tickers = ["AAPL"]
        await source.add_ticker("tsla")   # normalizes to uppercase
        assert "TSLA" in source.get_tickers()

    async def test_add_ticker_duplicate_is_noop(self):
        source, _ = _make_source()
        source._tickers = ["AAPL"]
        await source.add_ticker("AAPL")
        assert source.get_tickers().count("AAPL") == 1

    async def test_remove_ticker_clears_cache(self):
        source, cache = _make_source()
        cache.update("TSLA", 250.00)
        source._tickers = ["AAPL", "TSLA"]
        await source.remove_ticker("TSLA")
        assert "TSLA" not in source.get_tickers()
        assert cache.get("TSLA") is None

    async def test_empty_tickers_skips_fetch(self):
        source, _ = _make_source()
        source._tickers = []

        with patch.object(source, "_fetch_snapshots") as mock_fetch:
            await source._poll_once()
            mock_fetch.assert_not_called()
```

### 14.6 Unit tests — factory

**File: `backend/tests/market/test_factory.py`**

```python
import os
from unittest.mock import patch

from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


def test_no_api_key_returns_simulator():
    cache = PriceCache()
    with patch.dict(os.environ, {"MASSIVE_API_KEY": ""}, clear=False):
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)


def test_whitespace_api_key_returns_simulator():
    cache = PriceCache()
    with patch.dict(os.environ, {"MASSIVE_API_KEY": "   "}, clear=False):
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)


def test_api_key_set_returns_massive():
    cache = PriceCache()
    with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key-123"}, clear=False):
        source = create_market_data_source(cache)
    assert isinstance(source, MassiveDataSource)
```

---

## 15. Error Handling Reference

### Startup: empty watchlist

Both sources handle an empty ticker list gracefully. The simulator produces no prices, Massive skips its API call. SSE sends no-data events. When the user adds the first ticker, tracking starts immediately.

### Price cache miss during trade

If a user tries to buy/sell a ticker with no cached price (just added, Massive hasn't polled yet):

```python
price = price_cache.get_price(trade.ticker)
if price is None:
    raise HTTPException(
        status_code=400,
        detail=(
            f"Price not yet available for {trade.ticker}. "
            "Wait a moment and try again."
        ),
    )
```

The simulator avoids this via immediate cache seeding in `add_ticker()`. Massive may have a brief gap (up to `poll_interval` seconds) — the HTTP 400 with a human-readable message is the correct behavior.

### Massive API errors

| HTTP status | Cause | Behavior |
|-------------|-------|----------|
| 401 | Invalid API key | Logged as error; poller keeps retrying |
| 403 | Plan restriction | Logged; real-time data needs Advanced plan |
| 429 | Rate limited | Logged; next poll retries after `poll_interval` |
| 5xx | Server error | Massive client retries 3× internally; then logged |
| Network error | Timeout/DNS | Logged; retries on next cycle |

In all error cases, the cache retains last-known prices. SSE keeps streaming stale data — better than going blank.

### Cholesky decomposition failure

If an unusual correlation matrix is near-singular (rare with known tickers), the `1e-8 × I` regularization nudge in `_rebuild_cholesky()` prevents `np.linalg.cholesky` from raising `LinAlgError`. No exception propagates.

### SSE client disconnect

`request.is_disconnected()` is polled each iteration. When it returns `True`, the generator exits cleanly, releasing the HTTP connection. FastAPI handles the `asyncio.CancelledError` that may arrive when the ASGI server closes the response.

---

## 16. Configuration Summary

All tunable parameters and their defaults:

| Parameter | Where | Default | Description |
|-----------|-------|---------|-------------|
| `MASSIVE_API_KEY` | Environment variable | `""` (empty) | Set to use real market data; empty uses simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` s | Simulator tick rate |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` s | Massive API poll interval |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Chance of random shock per ticker per tick |
| `dt` | `GBMSimulator.DEFAULT_DT` | `~8.48e-8` | GBM time step (fraction of trading year) |
| SSE push interval | `_generate_events()` | `0.5` s | How often SSE checks for cache changes |
| SSE retry directive | `_generate_events()` | `1000` ms | EventSource reconnection delay |

### Quick-start usage

```python
from app.market import PriceCache, create_market_data_source

# At app startup
cache = PriceCache()
source = create_market_data_source(cache)   # reads MASSIVE_API_KEY env var
await source.start(["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
                    "NVDA", "META", "JPM", "V", "NFLX"])

# Read prices (any time, from any coroutine or thread)
update = cache.get("AAPL")          # PriceUpdate | None
price  = cache.get_price("AAPL")    # float | None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

# Dynamic watchlist
await source.add_ticker("PYPL")
await source.remove_ticker("GOOGL")

# At app shutdown
await source.stop()
```
