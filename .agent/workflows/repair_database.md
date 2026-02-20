---
description: How to repair the local market_data.db from Turso
---

Use this workflow if you suspect your local database or GDrive backup is missing data that exists on the remote Turso server.

1. Ensure your environment is set up and Infisical is connected.
2. Run the repair script:
```bash
PYTHONPATH=. python3 -c "from tools.migrate_historical_turso import repair_local_from_turso, get_remote_client, get_local_client; from src.infisical_manager import InfisicalManager; mgr = InfisicalManager(); remote = get_remote_client(mgr); local = get_local_client(); repair_local_from_turso(remote, local)"
```

Alternatively, you can run a full migration to ensure every single row matches:
```bash
PYTHONPATH=. python3 tools/migrate_historical_turso.py
```

3. The system will detect gaps and pull missing rows from Turso into `market_data.db`.
4. It will then automatically sync the repaired database to Google Drive on the next harvest run.
