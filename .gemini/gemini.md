# Analyst Workbench: AI Instructions & System Architecture

This document serves as the "System Knowledge Base" for the AI Agent (Antigravity) and human developers. It defines the core philosophy, infrastructure, and analytical rules engine.

## 1. Secrets Management (Infisical)

The project uses **Infisical** as the single source of truth for secrets (Turso URLs, API Keys, Webhooks).

### A. The SDK & Implementation
*   **Correct Package**: Always use `infisical-sdk`. **DO NOT** use the deprecated `infisical-python` package.
*   **Manager Pattern**: All logic is encapsulated in `modules/core/infisical_manager.py`. It initializes the client and handles authentication state.
*   **Usage**: `config.py` initializes the manager and fetches secrets during application startup.

### B. Authentication Methods
The manager supports two distinct authentication flows via environment variables:
1.  **Service Token (Legacy/Simple)**:
    *   Requires: `INFISICAL_TOKEN`.
    *   Auth Call: `client.auth.login(token=INFISICAL_TOKEN)`.
2.  **Universal Auth (Machine Identity - Preferred)**:
    *   Requires: `INFISICAL_CLIENT_ID`, `INFISICAL_CLIENT_SECRET`.
    *   Auth Call: `client.auth.universal_auth.login(client_id=..., client_secret=...)`.
*   **Required for both**: `INFISICAL_PROJECT_ID`.

### C. Secret Retrieval Flow
To fetch a secret, the manager uses:
```python
secret = client.secrets.get_secret_by_name(
    secret_name="NAME",
    project_id=PROJECT_ID,
    environment_slug="dev",
    secret_path="/"
)
```
*   **Environment**: Defaults to `dev`.
*   **Fallback Logic**: The system is designed with a "Waterfall Fallback":
    1. Try Infisical (Exact Name).
    2. Try Infisical (Simplified Name).
    3. Try local Environment Variables (`os.getenv`).

### D. GitHub Actions Integration
Secrets must be passed to the runner via the `env` block in the workflow YAML.
```yaml
env:
  INFISICAL_CLIENT_ID: ${{ secrets.INFISICAL_CLIENT_ID }}
  INFISICAL_CLIENT_SECRET: ${{ secrets.INFISICAL_CLIENT_SECRET }}
  INFISICAL_PROJECT_ID: ${{ secrets.INFISICAL_PROJECT_ID }}
```
If these are missing, the app logs a warning and enters "Offline/Legacy Mode."

---

## 2. CLI Operational Mandates (Gemini CLI ONLY)

The following rules apply **EXCLUSIVELY** to the **Gemini CLI** agent (this interface). They do **NOT** apply to automated agents like Antigravity.

1.  **Automatic Pushing**: Because all actions in the Gemini CLI are directed and approved by the user in real-time, the agent must **always** execute a `git push` immediately after completing a code modification or bug fix. 
2.  **No Manual Staging Required**: The agent should assume that once a task is finished, the state is ready for the remote repository.
3.  **Database Parity (Mirroring)**: The **Archive** and **Mirror** databases must remain 1-on-1 identical at all times for metadata and schema changes. Any modification made to the Archive database (e.g., updating `symbol_map`, renaming columns, or altering schema) MUST be immediately reflected in the Mirror database, whether explicitly requested or not.
