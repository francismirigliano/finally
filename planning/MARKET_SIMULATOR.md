# Market Simulator Design

Approach and code structure for simulating realistic stock prices when no Massive API key is configured. The simulator is the default and intended primary path — the Massive integration is an optional enhancement.

## Overview

The simulator uses **Geometric Brownian Motion (GBM)** to generate continuous price paths for multiple tickers simultaneously. GBM is the mathematical foundation of Black-Scholes pricing. Prices are always positive (the model is multiplicative), exhibit lognormal distributions consistent with real equities, and can be parameterized per ticker to reflect real-world volatility profiles.

Updates run every 500ms, producing a stream that feels live. Random shock events add drama — sudden 2–5% moves that trigger the frontend's price flash animations.

---

## GBM Math

At each time step, a stock price evolves as:

```
S(t+dt) = S(t) * exp((μ - σ²/2) * dt + σ * √dt * Z)
```

Where:
- `S(t)` — current price
- `μ` (mu) — annualized drift (expected return), e.g. `0.05` = 5%/yr
- `σ` (sigma) — annualized volatility, e.g. `0.25` = 25%/yr
- `dt` — time step as a fraction of a trading year
- `Z` — standard normal random variable drawn from N(0,1)

**Calibrating `dt` for 500ms updates**:
```
Trading year ≈ 252 days × 6.5 hours/day × 3600 seconds/hour = 5,896,800 seconds
dt = 0.5 / 5,896,800 ≈ 8.48e-8
```

This tiny `dt` produces sub-cent moves per tick — realistic per-step noise that accumulates into plausible intraday ranges over a simulated session.

**Why GBM and not something else**: GBM is the standard model taught alongside this project, it's computationally trivial, and its properties (positive prices, continuous paths, fat-ish tails from lognormal distribution) produce visually convincing price charts. It's not a model for real trading — it's a model for a compelling demo.

---

## Correlated Moves

Real stocks don't move independently. Tech stocks trend together during broad market moves. We generate correlated random draws using **Cholesky decomposition** of a correlation matrix.

Given a correlation matrix `C` for `n` tickers, compute `L = cholesky(C)`. For a vector of independent standard normals `Z_ind`:
```
Z_corr = L @ Z_ind
```

The resulting `Z_corr` has the covariance structure of `C`.

### Correlation Groups

| Group | Tickers | Within-group ρ |
|-------|---------|----------------|
| Tech | AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX | 0.65 |
| Finance | JPM, V | 0.55 |
| Tech ↔ Finance | cross-group | 0.30 |
| TSLA | standalone | 0.25 with everything |
| Unknown tickers | added dynamically | 0.30 with all |

The correlation matrix must be positive semi-definite for Cholesky to work. All values above produce valid matrices. Cholesky is recomputed whenever tickers are added or removed — O(n²) but n is small (<50 tickers).

---

## Random Shock Events

Every step, each ticker has a 0.1% probability (`p=0.001`) of a shock event — a sudden 2–5% move in either direction. This serves the demo's dramatic value and triggers the frontend's price flash animations at a human-noticeable rate.

Expected shock frequency: `1 / (0.001 × 2 ticks/sec)` ≈ one shock per ticker every 500 seconds (~8 minutes). With 10 tickers active, expect a shock somewhere roughly every 50 seconds.

```python
if random.random() < event_probability:
    shock_magnitude = random.uniform(0.02, 0.05)
    shock_direction = random.choice([-1, 1])
    price *= (1 + shock_magnitude * shock_direction)
```

---

## Seed Prices

Realistic starting prices for the default watchlist, calibrated to approximate real-world price ranges. The simulator does not try to track actual prices — these are just plausible starting points.

```python
SEED_PRICES: dict[str, float] = {
    "AAPL":  190.0,
    "GOOGL": 175.0,
    "MSFT":  420.0,
    "AMZN":  185.0,
    "TSLA":  250.0,
    "NVDA":  800.0,
    "META":  500.0,
    "JPM":   195.0,
    "V":     280.0,
    "NFLX":  600.0,
}
```

Tickers added dynamically that aren't in the seed table start at a random price between $50 and $300, sampled uniformly.

---

## Per-Ticker Parameters

Each known ticker has calibrated volatility. Higher-sigma tickers produce more dramatic price action. All drift values are set to a modest positive `μ = 0.04–0.08` to prevent prices from drifting to zero over a long session.

```python
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.55, "mu": 0.03},   # High vol, TSLA does its own thing
    "NVDA":  {"sigma": 0.45, "mu": 0.08},   # High vol, strong positive drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},   # Low vol bank stock
    "V":     {"sigma": 0.16, "mu": 0.04},   # Low vol payments stock
    "NFLX":  {"sigma": 0.38, "mu": 0.05},
}

DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}
```

---

## Implementation

### `GBMSimulator` class

```python
import math
import random
import numpy as np
from .seed_prices import SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS

# Correlation group definitions
_TECH = frozenset({"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"})
_FINANCE = frozenset({"JPM", "V"})


class GBMSimulator:
    """Generates correlated GBM price paths for a dynamic set of tickers."""

    def __init__(
        self,
        tickers: list[str],
        dt: float = 8.48e-8,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._tickers: list[str] = []
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self.add_ticker(ticker)

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker. No-op if already present."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = dict(TICKER_PARAMS.get(ticker, DEFAULT_PARAMS))
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker. No-op if not present."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance one time step. Returns {ticker: new_price} for all active tickers."""
        n = len(self._tickers)
        if n == 0:
            return {}

        # Correlated standard normal draws
        z_ind = np.random.standard_normal(n)
        z = self._cholesky @ z_ind if self._cholesky is not None else z_ind

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]

            drift = (mu - 0.5 * sigma ** 2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * float(z[i])
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random shock
            if random.random() < self._event_prob:
                shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                self._prices[ticker] *= (1.0 + shock)

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def _rebuild_cholesky(self) -> None:
        """Recompute the Cholesky factor of the correlation matrix."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n, dtype=float)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho

        # Add a small regularization term to ensure positive definiteness
        # This prevents Cholesky failure when unknown tickers produce a near-singular matrix
        corr += np.eye(n) * 1e-8
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _correlation(t1: str, t2: str) -> float:
        """Pairwise correlation coefficient for two tickers."""
        if t1 == t2:
            return 1.0
        t1_tech = t1 in _TECH
        t2_tech = t2 in _TECH
        t1_fin = t1 in _FINANCE
        t2_fin = t2 in _FINANCE

        if t1 == "TSLA" or t2 == "TSLA":
            return 0.25
        if t1_tech and t2_tech:
            return 0.65
        if t1_fin and t2_fin:
            return 0.55
        return 0.30    # cross-sector or unknown
```

---

## Seed Data Module

```python
# backend/app/market/seed_prices.py

SEED_PRICES: dict[str, float] = {
    "AAPL":  190.0,
    "GOOGL": 175.0,
    "MSFT":  420.0,
    "AMZN":  185.0,
    "TSLA":  250.0,
    "NVDA":  800.0,
    "META":  500.0,
    "JPM":   195.0,
    "V":     280.0,
    "NFLX":  600.0,
}

TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.55, "mu": 0.03},
    "NVDA":  {"sigma": 0.45, "mu": 0.08},
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},
    "V":     {"sigma": 0.16, "mu": 0.04},
    "NFLX":  {"sigma": 0.38, "mu": 0.05},
}

DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}
```

---

## Behavior Notes

**Prices are always positive**: GBM is multiplicative — `exp(...)` is always positive, so prices never go negative or reach zero regardless of how long the simulation runs.

**Intraday range plausibility**: With `sigma=0.25` and a 6.5-hour simulated day, the expected intraday range (high − low as % of open) is approximately `σ × √(1/252) ≈ 1.6%`. For TSLA at `sigma=0.55` that's ~3.5%. These match observed real-world intraday ranges.

**Positive semi-definiteness**: The correlation matrix is always valid as constructed (all off-diagonal values are in [0,1] and the matrix is symmetric). The `1e-8` identity nudge prevents rare numerical failure with near-singular matrices when unknown tickers are added.

**Cholesky rebuild cost**: O(n³) for decomposition, O(n²) for correlation matrix construction. With n < 50, this completes in microseconds even on slow hardware.

**Daily reference price**: The simulator does not maintain a "day open" reference price. The frontend computes session change as delta from page-load price, not from a market open. This is documented as expected behavior — the UI label should say "Since page load" rather than "Today's change."

**Shock frequency calibration**: At 0.001 probability per step and 2 steps/second, each ticker shocks roughly once every 8 minutes. With 10 tickers, the dashboard shows a shock roughly every 50 seconds — enough to keep the demo visually engaging without overwhelming the user.

---

## File Structure

```
backend/
  app/
    market/
      simulator.py      # GBMSimulator class
      seed_prices.py    # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS
```

`SimulatorDataSource` (the `MarketDataSource` implementation wrapping `GBMSimulator` in an async loop) lives in `market/simulator.py` alongside `GBMSimulator`, or in `market/interface.py` — see `MARKET_INTERFACE.md`.

---

## Testing the Simulator

The simulator is pure Python with deterministic behavior when seeded:

```python
import numpy as np
import random

np.random.seed(42)
random.seed(42)

sim = GBMSimulator(tickers=["AAPL", "MSFT"])
prices_t0 = sim.step()
prices_t1 = sim.step()

# AAPL and MSFT should move in the same direction more often than not (corr=0.65)
assert prices_t0["AAPL"] > 0
assert prices_t0["MSFT"] > 0
```

Unit tests should verify:
- Prices are always positive after many steps
- GBM formula produces the correct expected return and variance for known inputs
- Cholesky rebuild succeeds when tickers are added and removed dynamically
- Shock events alter price by 2–5%
- Unknown tickers receive DEFAULT_PARAMS and a seed price in [50, 300]
