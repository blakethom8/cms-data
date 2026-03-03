# GitHub Setup Instructions for CMS Data Pipeline

## Steps to Deploy

1. **Create a new repo on GitHub:**
   - Go to https://github.com/new
   - Repository name: **cms-healthcare-data** (or your preferred name)
   - Description: "Healthcare data infrastructure: 90M+ rows from CMS public datasets"
   - Make it **Public** (to showcase on your profile)
   - Do NOT initialize with README (we already have one)
   - Click "Create repository"

2. **Push this repo:**
   ```bash
   cd ~/Repo/cms-data
   git remote add origin https://github.com/blakethom8/cms-healthcare-data.git
   git branch -M main
   git push -u origin main
   ```

3. **Verify:**
   - Visit https://github.com/blakethom8/cms-healthcare-data
   - README should display with architecture diagrams and use cases

## Important Notes

- **.gitignore is configured** to exclude data files, credentials, and sensitive info
- **Data files stay local** — only code and documentation go to GitHub
- **API endpoint** in README uses production IP (5.78.148.70) — change if needed
- **License:** Currently set to "Private — All rights reserved" (change in README if you want to open source)

## Optional: Add Topics

After creating the repo, add topics to help discovery:
- `healthcare`
- `cms-data`
- `provider-intelligence`
- `data-pipeline`
- `duckdb`
- `fastapi`
- `medicare`

(Settings → Topics on GitHub repo page)
