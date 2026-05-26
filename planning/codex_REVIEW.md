# Codex Review of `planning/PLAN.md`

## Overall Assessment

This is a strong, coherent product plan with a clear demo story: live-feeling market data, simulated trading, and an AI assistant that can both analyze and act. The architecture choices are mostly well-matched to the stated goals, especially the emphasis on a single-container, single-user, low-friction setup.

The main thing missing is not vision, but sharper implementation boundaries. A few parts of the spec are still ambiguous enough that different agents could make incompatible assumptions, especially around the LLM execution flow, SSE payloads, chart/history behavior, and first-run behavior when API keys are absent.

## What Is Working Well

- **Clear product vision**: the app has a strong identity and a good capstone/demo shape.
- **Good architectural restraint**: FastAPI + static Next.js + SQLite + SSE is a sensible stack for this scope.
- **Practical simplifications**: market orders only, no auth, single port, and optional real market data are all good choices.
- **Good UX direction**: the plan makes the intended feel of the application easy to understand.
- **Reasonable persistence model**: portfolio state and chat history are persisted, while market data stays mostly ephemeral.

## Highest-Priority Gaps to Resolve

### 1. First-run experience vs. required API key
The plan promises a one-command launch with immediate usability, but `OPENROUTER_API_KEY` is currently listed as required.

**Recommendation:** make first launch work without any external keys.
- If no OpenRouter key is present, default to `LLM_MOCK=true` behavior or show chat in a clearly disabled/mock state.
- Keep the simulator as the default market data path.

This preserves the onboarding promise and reduces setup friction.

### 2. Define the API and SSE contracts explicitly
Several frontend requirements depend on backend payload details that are only described informally.

**Recommendation:** create a shared contract doc or schema for:
- `GET /api/watchlist`
- `GET /api/portfolio`
- `GET /api/portfolio/history`
- `POST /api/portfolio/trade`
- `POST /api/chat`
- `GET /api/stream/prices`

For SSE specifically, define:
- whether updates are batched or one-event-per-ticker
- exact JSON payload shape
- cadence expectations
- reconnect behavior
- how removed watchlist items and held positions affect streamed symbols

A batched payload is likely the simplest option, for example:

```json
{
  "type": "price_batch",
  "timestamp": "2026-05-22T12:00:00Z",
  "prices": [
    {
      "ticker": "AAPL",
      "price": 191.42,
      "previous_price": 191.10,
      "change_direction": "up"
    }
  ]
}
```

### 3. Tighten LLM action rules
The LLM is allowed to auto-execute trades and manage the watchlist, which is fine for a simulated environment, but the execution boundary needs to be very explicit.

**Recommendation:** define firm rules for when actions are allowed.
- Execute trades only on direct user intent, not on speculative or advisory responses.
- Allow watchlist changes only when the user asks or clearly agrees.
- Treat malformed or partial structured output as a non-actionable assistant response.
- Always persist an audit record of requested actions, attempted actions, validation failures, and executed actions.

This should be treated as one backend-owned workflow, not split loosely across chat and portfolio logic.

### 4. Clarify chart/history expectations
The plan mixes live frontend-accumulated price history with backend-stored portfolio snapshots, but it does not fully define chart behavior.

**Recommendation:** explicitly state:
- sparklines reset on page reload and are accumulated client-side only
- the main ticker chart is also client-side only for MVP, unless backend persistence is added later
- `portfolio_snapshots` are the only persisted time series in MVP
- the frontend should cap retained points per symbol for memory/performance reasons

Without this, agents may overbuild price-history persistence too early.

### 5. Prioritize MVP vs. polish features
The plan includes multiple rich visualizations, but not all of them are equally essential to proving the product.

**Recommendation:** define phased delivery.

**MVP**
- watchlist with live prices
- selected ticker main chart
- buy/sell workflow
- positions table
- cash and total portfolio value
- chat panel
- persisted trade history and portfolio snapshots

**Phase 2 / polish**
- heatmap/treemap
- advanced sparkline behavior
- extra animation polish
- optional cloud deployment stretch goals

This will make agent sequencing and review much easier.

## Important Clarifications Still Needed

### Trade rules
The trade system should specify:
- whether short selling is forbidden
- minimum quantity
- decimal precision for fractional shares
- ticker normalization rules
- behavior when a sell leaves a near-zero remainder because of floating-point issues
- whether trades use the latest cached price at request time or a separately fetched fill price

### Portfolio valuation rules
Define:
- `total_value = cash + sum(position quantity * latest price)`
- what happens if a held ticker has no cached price yet
- whether unrealized P&L uses current mark minus average cost

### Daily change percent
The watchlist includes daily change %, but the simulator description only guarantees latest and previous prices.

**Recommendation:** add a reference/open price field to the simulator and price cache so daily % change has a consistent definition.

### Database initialization timing
The plan says initialization happens on startup **or** first request.

**Recommendation:** choose one. Startup initialization is easier to reason about in tests, health checks, and container behavior.

### Static Next.js export limits
Because the frontend is a static export, the plan should explicitly forbid features that require a running Next.js server.

**Recommendation:** note that MVP should avoid:
- server actions
- route handlers
- server-side rendering dependencies
- runtime image optimization

## Suggestions to Simplify Delivery

- **Build simulator-first** and add Massive only after the full portfolio/chat loop works.
- **Use one charting library** for all charts unless there is a strong reason not to.
- **Keep user handling fully constant** in code via something like `DEFAULT_USER_ID`.
- **Do not persist ticker history in MVP** unless a later requirement truly needs it.
- **Document one golden-path startup flow** and treat everything else as optional convenience.

## Recommended Next Documents

To reduce coordination mistakes, the project would benefit from adding:

1. **`planning/API_CONTRACT.md`**  
   Request/response examples and SSE event schemas.

2. **`planning/MVP_SCOPE.md`**  
   A hard boundary between must-have and stretch features.

3. **`planning/LLM_ACTION_POLICY.md`**  
   Exact rules for prompting, structured outputs, validation, execution, and error handling.

4. **`planning/STATE_MODEL.md`**  
   Source-of-truth rules for price cache, watchlist contents, positions, portfolio valuation, and chart history.

## Bottom Line

The plan is good and already stronger than most early-stage specs because it has a clear product shape and intentionally simple architecture. The biggest risk is not technical feasibility; it is ambiguity at the boundaries between frontend, backend, streaming, and LLM action execution.

If those contracts are tightened before implementation proceeds too far, this should be a very buildable project.
