# FinAlly - Architecture & Plan Review

## 1. Overall Impression
The plan for FinAlly is well-structured, clear, and perfectly scoped for an AI coding capstone project. The choice of a single container with a static Next.js frontend served by FastAPI, combined with an embedded SQLite database and Server-Sent Events (SSE), significantly reduces deployment and operational complexity.

## 2. Strengths
*   **Simplicity first:** The choice of SSE over WebSockets and SQLite over Postgres avoids unnecessary infrastructure overhead while providing the required real-time features.
*   **Clear Boundaries:** The separation of concerns between frontend (Next.js), backend (FastAPI/uv), and the database is well-defined.
*   **Mocking & Testing:** Built-in market simulation and `LLM_MOCK` mode are excellent choices for ensuring the app can be developed, tested, and demonstrated offline or without requiring external API keys.
*   **UX focus:** The deliberate choice to auto-execute trades without confirmation makes for a fluid demo and showcases agentic capabilities effectively.

## 3. Potential Risks & Recommendations

### A. Database Initialization
*   **Risk:** The plan mentions "lazy initialization" of the SQLite database on startup or the *first request*. Initializing on the first request can lead to race conditions if multiple requests (e.g., frontend loading and simultaneously requesting portfolio, watchlist, and SSE stream) hit the server concurrently before the schema is fully created.
*   **Recommendation:** Initialize the database explicitly during the FastAPI application startup lifecycle (e.g., using the `lifespan` context manager) rather than lazily on the first request.

### B. State Management & SSE
*   **Risk:** The SSE stream pushes updates at ~500ms intervals. While fine for the backend, React/Next.js frontend state management needs to be carefully handled to avoid excessive React tree re-renders that could degrade browser performance.
*   **Recommendation:** As noted in the plan, use a specialized canvas-based charting library (like Lightweight Charts). Furthermore, consider keeping the fast-moving price state outside of standard React state where possible (e.g., using refs or direct DOM manipulation for the green/red "flash" effect) to maintain 60fps performance.

### C. LLM Structured Output Failure
*   **Risk:** LiteLLM / OpenRouter might return malformed JSON, partial schemas, or experience timeouts.
*   **Recommendation:** Ensure the backend implements robust fallback mechanisms. If the LLM returns invalid JSON, the backend should ideally retry the request (with a strict retry limit) or gracefully inform the user that the action couldn't be parsed, without crashing the chat thread or corrupting the `chat_messages` table.

### D. Trade Execution Concurrency
*   **Risk:** Instant market orders could lead to negative balances or overselling if the LLM issues multiple conflicting trades rapidly, or if the user clicks "buy" while the LLM is simultaneously executing a trade.
*   **Recommendation:** Use standard SQLite transactions (`BEGIN EXCLUSIVE` or FastAPI/SQLAlchemy transaction scopes) when executing trades to ensure cash balance and position quantity integrity under concurrent actions.

## 4. Conclusion
The architecture is highly viable and well-reasoned. The emphasis on a one-command Docker setup and fallback mocks ensures it will be a reliable, demo-ready application. Focusing on the simulator MVP first (as suggested in the simplification opportunities) is the right path forward.