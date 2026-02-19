# Proptech Operations MVP

Minimal web MVP implementing broker workflow, RM control queues, and verified visit completion using OTP + geo/photo fallback.

## Stack

- Python 3 standard library (`http.server`, `sqlite3`) only
- SQLite app database (`/Users/viditkiyal/Documents/New project/proptech-mvp/data/app.db`)
- Vanilla HTML/CSS/JS frontend
- Excel-compatible CSV sync every 30 minutes (`/Users/viditkiyal/Documents/New project/proptech-mvp/data/leads_import.csv`)

## Run (Layman steps)

1. Open the **Terminal** app on your Mac.
2. Copy-paste this and press Enter:

```bash
cd "/Users/viditkiyal/Documents/New project/proptech-mvp"
```

3. Then copy-paste this and press Enter:

```bash
python3 server.py
```

4. Keep this terminal window open.
5. Open your browser and go to: <http://127.0.0.1:8080>
6. Login using one of the demo users below.

To stop the app later: go back to Terminal and press `Control + C`.

Alternative: double-click `/Users/viditkiyal/Documents/New project/proptech-mvp/start_mvp.command`.

## Run (quick command for technical users)

```bash
cd "/Users/viditkiyal/Documents/New project/proptech-mvp" && python3 server.py
```

For hosting/deployment platforms, server now supports:

- `HOST` env var (example: `0.0.0.0`)
- `PORT` env var (example: platform-provided port)

## Demo users

- `broker.jaipur@example.com / broker123`
- `broker.nagpur@example.com / broker123`
- `rm.jaipur@example.com / rm123`
- `rm.nagpur@example.com / rm123`
- `srm.ops@example.com / srm123`

## Rules implemented from your decisions

- Duplicate handling:
  - Similarity `>75%`: hidden from customers + RM review queue
  - Similarity `>95%`: auto-hidden + RM review queue (not auto-deleted)
- Workflow source of truth:
  - App DB owns workflow state
  - Excel is lead import/export input only
- RM emergency SLA:
  - Raised before 12:00 PM: resolve within 12 hours
  - Raised after 12:00 PM: resolve within 24 hours
  - Missed SLA escalates to SRM queue
- Unique visit rule:
  - Only first-ever completed visit (customer phone based) is `unique`
- Visit completion proof:
  - OTP (2 minutes expiry, max 3 attempts) + geo check within 200 meters
  - If geo unavailable/fails, photo fallback is allowed
- Slot cancellation:
  - Broker can cancel slot, cannot reject customer directly
  - Booked visit cancellation <24 hours:
    - apology event + priority rebook window 48 hours + RM call event
    - emergency flow to RM/SRM review, flag if rejected
  - Booked visit cancellation >=24 hours:
    - apology event, no priority rebook
- Flags:
  - 1st flag warning
  - 2nd flag warning + monthly incentive-block marker
  - 3rd flag broker deactivated
  - each flag decays in 90 days
- Multi-property same-broker duration:
  - first property = 120 minutes, each additional property = 45 minutes
  - API: `GET /api/scheduling/duration?property_count=N`
- Customer self-service:
  - customer can cancel and reschedule using phone number + visit id
  - reschedule allows primary broker slots first and backup broker slots if mapped
- WhatsApp integration layer (mock provider):
  - template store + outbound message log + inbound webhook log
  - booking, OTP, cancellation, and reschedule events queue WhatsApp messages
  - webhook commands supported: `HELP`, `CANCEL <visit_id>`, `RESCHEDULE <visit_id> <slot_id>`
- Reports and exports:
  - funnel report, broker reliability report
  - CSV exports for visit counts, funnel, reliability, WhatsApp logs, visits

## Current MVP screens

- Broker:
  - Inventory add/remove
  - Site visit list
  - Slot calendar add/cancel
  - Send OTP and complete visit
- RM:
  - Duplicate review queue
  - Emergency approval queue
  - Lead import monitor
  - Quick booking form for demo
  - WhatsApp test send + message log
  - Report dashboard + CSV exports
- SRM:
  - Escalation queue
- Customer self-service:
  - load visits by phone
  - cancel and reschedule from available slots

## Important MVP limitations

- WhatsApp and call triggers are represented as event logs, not real provider integrations
- Duplicate scoring is heuristic and should be replaced with better image/location matching in production
- No file storage service: photo fallback is stored as base64 in DB for demo
- No legal/compliance module in this MVP

## Share with people outside your laptop/Codex

1. Share the workflow/chart document:
- File: `/Users/viditkiyal/Documents/New project/proptech-mvp/WORKFLOW_CHART.md`
- You can copy into Notion/Google Docs or export to PDF.

2. Quick temporary public demo link (your laptop must stay on):
- Install Cloudflare tunnel: `brew install cloudflared`
- Start app: `python3 server.py`
- Open public tunnel: `cloudflared tunnel --url http://127.0.0.1:8080`
- Share the generated `https://...trycloudflare.com` link.

3. Persistent shareable URL (recommended):
- Push this folder to GitHub.
- Deploy on Render/Railway as a Python web service.
- Start command: `python3 server.py`
- Set `HOST=0.0.0.0` and `PORT` from platform environment.

## Option B (Made ready for you): Render deployment

This project already includes deployment files:

- `/Users/viditkiyal/Documents/New project/proptech-mvp/render.yaml`
- `/Users/viditkiyal/Documents/New project/proptech-mvp/requirements.txt`
- `/Users/viditkiyal/Documents/New project/proptech-mvp/Procfile`
- `/Users/viditkiyal/Documents/New project/proptech-mvp/.gitignore`

### Step 1: Put `proptech-mvp` on GitHub

1. Create a new GitHub repository (empty), e.g. `proptech-mvp`.
2. In Terminal, run:

```bash
cd "/Users/viditkiyal/Documents/New project/proptech-mvp"
git init
git add .
git commit -m "Initial deploy-ready MVP"
git branch -M main
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```

Replace `<YOUR_GITHUB_REPO_URL>` with the URL from your new GitHub repo page.

Or use the helper script:

```bash
cd "/Users/viditkiyal/Documents/New project/proptech-mvp"
./push_to_github.command <YOUR_GITHUB_REPO_URL>
```

### Step 2: Deploy on Render

1. Go to [Render](https://render.com) and login.
2. Click **New +** -> **Blueprint**.
3. Connect your GitHub account and select your `proptech-mvp` repo.
4. Render will detect `render.yaml` automatically.
5. Click **Apply** / **Create**.
6. Wait for build and deploy to complete.
7. Open the generated Render URL.

### Step 3: Login in live app

Use the same demo users:

- `broker.jaipur@example.com / broker123`
- `rm.jaipur@example.com / rm123`
- `srm.ops@example.com / srm123`

### Notes for deployment data

- Current config uses `DATA_DIR=/tmp/proptech-data` on Render.
- This is fine for demo MVP.
- If service restarts, data may reset; for production, attach persistent storage and point `DATA_DIR` there.

### If Render URL opens but shows `404 Not Found`

Use this quick diagnosis:

1. Open `<your-render-url>/api/health`
2. If this works but homepage does not, backend is up and static frontend path is wrong.

Fix in Render service settings:

1. Ensure branch is latest and redeploy after pushing newest code.
2. Ensure **Start Command** is:
   - `python3 server.py` (if repo root is `proptech-mvp`)
   - or `cd proptech-mvp && python3 server.py` (if `proptech-mvp` is a subfolder in repo)
3. Add env vars if needed:
   - `HOST=0.0.0.0`
   - `DATA_DIR=/tmp/proptech-data`
   - `STATIC_DIR=/opt/render/project/src/static` or `/opt/render/project/src/proptech-mvp/static`
4. Redeploy and recheck root URL.
