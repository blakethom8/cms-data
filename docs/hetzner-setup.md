# Hetzner Server Setup for Provider Data Pipeline

## Server Specifications

### Recommended Configuration: **CX52**

| Spec | Value | Rationale |
|------|-------|-----------|
| **vCPUs** | 8 dedicated | Parallel CSV processing, DuckDB analytical queries |
| **RAM** | 64 GB | NPPES (25GB uncompressed) + DuckDB working memory |
| **Storage** | 240 GB SSD | Raw data (~36GB) + DuckDB (~50GB) + workspace (~50GB) |
| **Network** | 20 TB traffic | Sufficient for monthly downloads + Supabase syncs |
| **Cost** | ~€46/month (~$50 USD) | |
| **Location** | Nuremberg, DE or Ashburn, VA | Choose based on latency to Supabase region |

**Alternative (budget option):** CX42 (8 vCPU, 32 GB RAM, €33/month) — will work but slower for NPPES processing.

---

## Initial Setup

### 1. Provision Server

```bash
# Via Hetzner Cloud Console or API
# Choose: CX52, Ubuntu 24.04 LTS, SSH key auth
```

### 2. Base System Configuration

```bash
# Update system
apt update && apt upgrade -y

# Install essentials
apt install -y \
    python3.12 \
    python3.12-venv \
    python3-pip \
    git \
    curl \
    wget \
    unzip \
    postgresql-client \
    htop \
    tmux \
    nginx

# Create data user (non-root)
useradd -m -s /bin/bash dataops
usermod -aG sudo dataops
su - dataops
```

### 3. Python Environment

```bash
# Clone repo
cd ~
git clone https://github.com/blakethom8/cms-data.git
cd cms-data

# Create venv
python3.12 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install duckdb requests pandas tqdm python-dotenv
```

### 4. Directory Structure

```bash
mkdir -p ~/cms-data/data/{raw,processed,exports}
mkdir -p ~/cms-data/logs
mkdir -p ~/cms-data/cache

# data/raw/        - downloaded CSVs
# data/processed/  - DuckDB database files
# data/exports/    - CSV/SQL dumps for Supabase sync
# logs/            - pipeline execution logs
# cache/           - partial downloads, temp files
```

### 5. Environment Variables

```bash
# ~/cms-data/.env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key
OPENAI_API_KEY=sk-...  # for LLM enrichment
DB_PATH=/home/dataops/cms-data/data/processed/provider_searcher.duckdb
RAW_DIR=/home/dataops/cms-data/data/raw
EXPORT_DIR=/home/dataops/cms-data/data/exports
```

### 6. Firewall Configuration

```bash
# Hetzner Cloud Firewall (via console)
# Allow inbound:
#   - SSH (22) from your IP only
#   - HTTP/HTTPS (80/443) for nginx status page (optional)
# Block all other inbound
# Allow all outbound (for downloads, API calls)
```

---

## Pipeline Installation

### Clone and Configure

```bash
cd ~/cms-data

# Verify pipeline structure
ls -la pipeline/
# acquire.py, load.py, transform.py, scoring.py, config.py

# Make scripts executable (if needed)
chmod +x pipeline/*.py

# Test DuckDB
python -c "import duckdb; print(duckdb.__version__)"
```

### Configuration Updates

Update `pipeline/config.py` to use Hetzner paths:

```python
# pipeline/config.py
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv('DATA_DIR', PROJECT_ROOT / "data"))
RAW_DIR = Path(os.getenv('RAW_DIR', DATA_DIR / "raw"))
EXPORT_DIR = Path(os.getenv('EXPORT_DIR', DATA_DIR / "exports"))
DB_PATH = Path(os.getenv('DB_PATH', DATA_DIR / "processed/provider_searcher.duckdb"))
```

---

## Automation

### Cron Jobs

```bash
# Edit crontab
crontab -e

# Weekly CMS refresh (Sundays 2 AM)
0 2 * * 0 /home/dataops/cms-data/scripts/weekly_refresh.sh >> /home/dataops/cms-data/logs/weekly_refresh.log 2>&1

# Monthly NPPES refresh (1st of month, 3 AM)
0 3 1 * * /home/dataops/cms-data/scripts/monthly_nppes.sh >> /home/dataops/cms-data/logs/monthly_nppes.log 2>&1

# Annual Open Payments refresh (July 1, 4 AM)
0 4 1 7 * /home/dataops/cms-data/scripts/annual_open_payments.sh >> /home/dataops/cms-data/logs/open_payments.log 2>&1

# Daily Supabase sync (6 AM)
0 6 * * * /home/dataops/cms-data/scripts/sync_to_supabase.sh >> /home/dataops/cms-data/logs/supabase_sync.log 2>&1
```

### Scripts to Create

#### `scripts/weekly_refresh.sh`
```bash
#!/bin/bash
set -e

cd /home/dataops/cms-data
source .venv/bin/activate

echo "[$(date)] Starting weekly CMS refresh..."

# Download latest CMS data
python -m pipeline.acquire

# Load into DuckDB
python -m pipeline.load

# Transform and score
python -m pipeline.transform
python -m pipeline.scoring

echo "[$(date)] Weekly refresh complete."
```

#### `scripts/monthly_nppes.sh`
```bash
#!/bin/bash
set -e

cd /home/dataops/cms-data
source .venv/bin/activate

echo "[$(date)] Starting monthly NPPES refresh..."

# Download NPPES (9GB, takes 20-30 min)
python -m pipeline.nppes --download

# Enrich core_providers
python -m pipeline.nppes --enrich

# Rebuild derived tables
python -m pipeline.transform
python -m pipeline.scoring

echo "[$(date)] NPPES refresh complete."
```

#### `scripts/sync_to_supabase.sh`
```bash
#!/bin/bash
set -e

cd /home/dataops/cms-data
source .venv/bin/activate
source .env

echo "[$(date)] Starting Supabase sync..."

# Export tables from DuckDB to CSV
python -m pipeline.export --tables core_providers,utilization_metrics,practice_locations,hospital_affiliations,provider_quality_scores

# Upload to Supabase via psql
for table in core_providers utilization_metrics practice_locations hospital_affiliations provider_quality_scores; do
    echo "Syncing $table..."
    psql "$SUPABASE_URL" \
        -c "TRUNCATE TABLE $table;" \
        -c "\COPY $table FROM '$EXPORT_DIR/$table.csv' WITH (FORMAT CSV, HEADER TRUE);"
done

echo "[$(date)] Supabase sync complete."
```

---

## Monitoring

### Disk Usage Monitoring

```bash
# Add to crontab (daily disk usage check)
0 8 * * * df -h /home/dataops/cms-data >> /home/dataops/cms-data/logs/disk_usage.log
```

### Process Monitoring

```bash
# Install and configure htop for real-time monitoring
htop

# Check DuckDB processes
ps aux | grep duckdb

# Monitor pipeline progress
tail -f ~/cms-data/logs/weekly_refresh.log
```

### Log Rotation

```bash
# /etc/logrotate.d/cms-data
/home/dataops/cms-data/logs/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 0640 dataops dataops
}
```

---

## Backup Strategy

### Database Backups

```bash
# Daily DuckDB backup (run at midnight)
0 0 * * * /home/dataops/cms-data/scripts/backup_db.sh >> /home/dataops/cms-data/logs/backup.log 2>&1
```

#### `scripts/backup_db.sh`
```bash
#!/bin/bash
set -e

BACKUP_DIR=/home/dataops/cms-data/backups
mkdir -p $BACKUP_DIR

DATE=$(date +%Y-%m-%d)
DB_PATH=/home/dataops/cms-data/data/processed/provider_searcher.duckdb

# Copy DuckDB file
cp $DB_PATH $BACKUP_DIR/provider_searcher_$DATE.duckdb

# Compress
gzip $BACKUP_DIR/provider_searcher_$DATE.duckdb

# Keep last 7 days
find $BACKUP_DIR -name "*.duckdb.gz" -mtime +7 -delete

echo "[$(date)] Backup complete: provider_searcher_$DATE.duckdb.gz"
```

### Off-Site Backups (Optional)

```bash
# Sync to Hetzner Storage Box or AWS S3
# Weekly off-site backup (Sundays, 8 AM)
0 8 * * 0 rclone sync /home/dataops/cms-data/backups/ hetzner-storage:cms-backups/
```

---

## Web Crawling Infrastructure

### For High-Value Provider Web Scraping

```bash
# Install Playwright (for browser automation)
pip install playwright beautifulsoup4 aiohttp
playwright install chromium

# Create crawling module
mkdir -p ~/cms-data/pipeline/crawlers
touch ~/cms-data/pipeline/crawlers/__init__.py
touch ~/cms-data/pipeline/crawlers/provider_web_crawler.py
```

**Crawling strategy:**
- Use Google Places API to get practice websites
- Playwright for JavaScript-heavy sites
- BeautifulSoup for static HTML
- Respect robots.txt
- Rate limiting: 1 request per 2 seconds per domain
- Parallel workers: 8 (one per vCPU)

**Storage:**
```bash
mkdir -p ~/cms-data/data/crawled/{html,parsed}
# html/    - raw HTML for archival
# parsed/  - extracted structured data (JSON)
```

---

## Security Hardening

### SSH Configuration

```bash
# /etc/ssh/sshd_config
PermitRootLogin no
PasswordAuthentication no
PubkeyAuthentication yes
AllowUsers dataops

# Restart SSH
systemctl restart sshd
```

### Fail2Ban

```bash
apt install -y fail2ban

# /etc/fail2ban/jail.local
[sshd]
enabled = true
maxretry = 3
bantime = 3600
```

### Automatic Security Updates

```bash
apt install -y unattended-upgrades
dpkg-reconfigure -plow unattended-upgrades
```

---

## Cost Estimate

| Component | Monthly Cost |
|-----------|--------------|
| Hetzner CX52 | €46 (~$50) |
| Hetzner Storage Box (optional, 1TB) | €3.81 (~$4) |
| Outbound bandwidth | Included (20TB) |
| **Total** | **~$54/month** |

**One-time setup time:** 2-3 hours

---

## Performance Expectations

| Task | Duration | Notes |
|------|----------|-------|
| Initial CMS download (5GB) | 15-20 min | Depends on CMS server speed |
| NPPES download (9GB) | 20-30 min | Monthly |
| Open Payments download (6GB) | 15-20 min | Annual |
| DuckDB load (all data) | 30-45 min | First run |
| Transform + scoring | 20-30 min | Incremental updates faster |
| Web crawl (1000 providers) | 30-60 min | 2 sec per site, 8 parallel workers |
| Export to Supabase | 10-15 min | ~8M rows across tables |
| **Total weekly refresh** | **~2 hours** | CMS data only |
| **Total monthly refresh** | **~4 hours** | CMS + NPPES |

---

## Supabase Sync Strategy

### Push vs. Pull

**Decision:** Push from Hetzner to Supabase (not pull)

**Method:**
1. DuckDB → CSV export (`pipeline/export.py`)
2. `psql` COPY command → Supabase PostgreSQL
3. Alternatively: Supabase REST API (slower, better logging)

### Incremental vs. Full Refresh

**Phase 1:** Full refresh (TRUNCATE + INSERT)
- Simpler to implement
- No conflict resolution needed
- Works for weekly/monthly cadence

**Phase 2:** Incremental sync (INSERT/UPDATE/DELETE)
- Track `last_modified` timestamp in DuckDB
- Compare with Supabase, sync only changes
- More efficient for large datasets

### Connection Setup

```bash
# Install Supabase CLI (optional, for easier mgmt)
npm install -g supabase

# Test connection
psql "postgresql://postgres:[PASSWORD]@db.[PROJECT].supabase.co:5432/postgres"
```

---

## Next Steps

1. **Provision Hetzner CX52** via Cloud Console
2. **Run initial setup** (steps 1-6 above)
3. **Clone cms-data repo** and configure
4. **Test pipeline manually** before automating
5. **Set up cron jobs** for weekly/monthly refreshes
6. **Configure Supabase sync** and test export/import

Want me to create the missing pipeline modules (`nppes.py`, `open_payments.py`, `export.py`) next?
