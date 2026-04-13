# Command Reference — Synthetic Data Generation Framework
## Plain-English Guide for Everyone

---

## Before You Run Anything

**Set your Groq API key once — never put it in commands or Postman:**
```bash
# Mac / Linux — paste this in your terminal once per session
export GROQ_API_KEY=gsk_xxxxxxxxxxxxxxxxxxxx

# Windows (Command Prompt)
set GROQ_API_KEY=gsk_xxxxxxxxxxxxxxxxxxxx

# Windows (PowerShell)
$env:GROQ_API_KEY="gsk_xxxxxxxxxxxxxxxxxxxx"

# To make it permanent (Mac/Linux) — add to your ~/.bashrc or ~/.zshrc:
echo 'export GROQ_API_KEY=gsk_xxxx' >> ~/.bashrc
```
> **Why?** The key is a secret — like a password. Setting it as an environment variable means it lives on the server only, never travels over the network, and never appears in Postman requests or terminal history.

---

## One-Time Setup

```bash
# Install all Python libraries the project needs
pip install -r requirements.txt
```

---

## CLI Commands

### DRY RUN — Generate CSV files only, do NOT touch the database
> Use this to safely preview what data will be generated before committing anything to the DB. 
> CSV files land in the `./output` folder. You can open them in Excel to inspect.

```bash
# PostgreSQL (reads connection from config.yaml)
python auto_pipeline.py --dry-run

# Oracle
python auto_pipeline.py --engine oracle --host localhost --service-name ORCL \
    --user system --password secret --dry-run

# SQL Server
python auto_pipeline.py --engine sqlserver --host localhost --db DatagenDB \
    --user sa --password secret --dry-run

# MySQL
python auto_pipeline.py --engine mysql --host localhost --db DatagenDB \
    --user root --password secret --dry-run
```

---

### FULL LOAD — Generate data AND load it into the database
> This is the real run. The pipeline reads your schema, generates T24-precise data,
> and bulk-loads it directly into your database.
> **Safe to re-run** — tables are automatically cleared before each load so you never
> get duplicate key errors.

```bash
# PostgreSQL (reads all settings from config.yaml — simplest option)
python auto_pipeline.py

# Oracle
python auto_pipeline.py --engine oracle --host localhost --service-name ORCL \
    --user system --password secret

# SQL Server
python auto_pipeline.py --engine sqlserver --host localhost --db DatagenDB \
    --user sa --password secret

# MySQL
python auto_pipeline.py --engine mysql --host localhost --db DatagenDB \
    --user root --password secret
```

---

### WITH SPECIFIC SEED — Reproduce the exact same data again
> Useful when a developer found a bug with a specific dataset and wants to recreate it exactly.

```bash
# Save this run under the name "sprint_42"
python auto_pipeline.py --seed-profile sprint_42

# Later — reproduce that exact same data
python auto_pipeline.py --seed-profile sprint_42
```

---

## FastAPI Server Commands
> These commands start the API server so Postman and the UI team can use the tool
> without ever touching the terminal.

### Start the server (normal mode)
> Starts the REST API. Open Postman or the browser and start sending requests.
> Keep this terminal window open while you work — closing it stops the server.

```bash
python auto_pipeline.py --serve
```
Then open: **http://localhost:8000/docs** — full interactive API docs in the browser.

---

### Start with hot reload (development / testing mode only)
> Same as above, but the server **automatically restarts** whenever you save a change
> to any Python file. Useful only when you are actively editing the code.
> Do NOT use this in production — it is slower and meant for developers only.

```bash
python auto_pipeline.py --serve --reload
```

---

### Start on a different port
> Use this if port 8000 is already taken by something else on your machine.

```bash
python auto_pipeline.py --serve --api-port 9000
# Then open: http://localhost:9000/docs
```

---

## Postman Setup (for UI Team)

1. Import `SyntheticDataGen_API.postman_collection.json` into Postman
2. Open the **collection variables** (click the collection → Variables tab)
3. Set `base_url` to `http://localhost:8000` (or wherever the server is running)
4. **Do NOT add a Groq key anywhere in Postman** — the server handles it internally
5. Run **Health Check** first — the response will show `"groq_key_configured": true`
   if the server is ready. If it shows `false`, the administrator needs to set the key.

### Typical Postman workflow:
```
1. Health Check               → confirm server is up and Groq key is set
2. Test Connection            → confirm DB is reachable
3. Read Schema                → preview tables that will be processed
4. Run Pipeline (Async)       → starts the job, saves job_id automatically
5. Get Job Status (poll)      → keep hitting this until status = "completed"
6. List Scenarios             → see auto-generated test scenarios
7. Run Scenario               → run a specific scenario (e.g. stress_test)
```

---

## Summary Table

| What you want to do | Command |
|---|---|
| Preview data without touching DB | `python auto_pipeline.py --dry-run` |
| Generate and load into PostgreSQL | `python auto_pipeline.py` |
| Generate and load into Oracle | `python auto_pipeline.py --engine oracle ...` |
| Generate and load into SQL Server | `python auto_pipeline.py --engine sqlserver ...` |
| Generate and load into MySQL | `python auto_pipeline.py --engine mysql ...` |
| Start the REST API server | `python auto_pipeline.py --serve` |
| Start server (dev/edit mode) | `python auto_pipeline.py --serve --reload` |
| Start server on different port | `python auto_pipeline.py --serve --api-port 9000` |
| Reproduce a previous exact run | `python auto_pipeline.py --seed-profile <name>` |