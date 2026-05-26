# FinAlly — AI Trading Workstation

A visually stunning AI-powered trading platform that streams live market data, simulates portfolio trading, and integrates an LLM chat assistant capable of analyzing positions and executing trades on your behalf.

Built as a capstone project demonstrating how orchestrated AI agents produce production-quality full-stack applications.

## Quick Start

```bash
# Clone the repo
git clone <repo-url>
cd finally

# Set up environment (see Configuration below)
cp .env.example .env
# Edit .env and add your OPENROUTER_API_KEY

# Run the application
./scripts/start_mac.sh      # macOS/Linux
./scripts/start_windows.ps1 # Windows
```

The app opens at `http://localhost:8000`. You get $10,000 in virtual cash and a watchlist of 10 default tickers with live-updating prices.

## What You Can Do

- **Watch prices stream** with subtle flash animations (green uptick, red downtick)
- **View sparklines** in the watchlist and detailed charts in the main area
- **Trade market orders** — instant fill at current price, no confirmation dialogs
- **Monitor your portfolio** via a heatmap treemap and P&L chart
- **Chat with the AI assistant** to analyze positions, ask for recommendations, and execute trades through natural language
- **Manage your watchlist** manually or via the assistant

## Architecture

Single Docker container on port 8000:

| Component | Tech | Purpose |
|-----------|------|---------|
| Frontend | Next.js (TypeScript, static export) | Single-page app served as static files |
| Backend | FastAPI (Python/uv) | REST API, SSE streaming, LLM integration |
| Database | SQLite | Portfolio state, trades, watchlist, chat history |
| Market Data | Simulator (default) or Massive API | Live-like price updates via SSE |
| LLM | LiteLLM → OpenRouter (Cerebras) | Structured chat with auto-executing trades |

See `planning/PLAN.md` for the full specification.

## Configuration

Create a `.env` file in the project root:

```bash
# Required: OpenRouter API key for chat functionality
OPENROUTER_API_KEY=your-key-here

# Optional: Massive (Polygon.io) API key for real market data
# If omitted, the built-in simulator runs instead (recommended for most use cases)
MASSIVE_API_KEY=

# Optional: Set to "true" for deterministic mock LLM responses (testing)
LLM_MOCK=false
```

## Project Structure

```
finally/
├── frontend/          # Next.js app (TypeScript)
├── backend/           # FastAPI app (Python/uv)
│   └── db/            # Schema and seed logic
├── planning/          # Project documentation (shared agent contract)
├── scripts/           # Start/stop scripts for Docker
├── test/              # E2E tests (Playwright)
├── Dockerfile         # Multi-stage build (Node → Python)
└── db/                # Runtime volume mount for SQLite
```

## Development

- **Frontend agent** owns `frontend/` and component architecture
- **Backend/Market Data agent** owns `backend/`, database schema, API routes, and market data
- **All agents** reference `planning/` as the shared contract and coordination point

## Testing

**Unit tests** live within `frontend/` and `backend/` following each framework's conventions.

**E2E tests** are in `test/` with Playwright and a separate `docker-compose.test.yml`.

Run tests with:
```bash
npm test          # Frontend (from frontend/)
pytest            # Backend (from backend/)
npm run test:e2e  # E2E (from project root)
```

## Deployment

The Docker image is production-ready and deploys to any container platform (AWS App Runner, Render, etc.). Use the provided start scripts for local development or build the image yourself:

```bash
docker build -t finally .
docker run -v finally-data:/app/db -p 8000:8000 --env-file .env finally
```

The SQLite database persists via a named volume across container restarts.

## Documentation

- **`planning/PLAN.md`** — Full specification (vision, UX, architecture, endpoints, database schema, LLM integration)
- **`planning/MARKET_DATA_SUMMARY.md`** — Market data simulator and Massive API integration details
- **`planning/archive/`** — Earlier design docs and evolution notes

## License

Part of an agentic AI coding course. See LICENSE for details.
