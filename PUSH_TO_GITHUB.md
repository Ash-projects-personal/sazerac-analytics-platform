# Push to GitHub

This repo is committed locally. To publish:

```bash
# 1. Create a new repo at github.com/new
#    Name: sazerac-analytics-platform
#    Visibility: Public (so GitHub Pages works)
#    Do NOT initialize with README

# 2. Add remote and push
git remote add origin https://github.com/YOUR_USERNAME/sazerac-analytics-platform.git
git branch -M main
git push -u origin main

# 3. Enable GitHub Pages
#    Settings → Pages → Source: GitHub Actions
#    The included workflow will auto-deploy the dashboard on push

# 4. Your live dashboard URL will be:
#    https://YOUR_USERNAME.github.io/sazerac-analytics-platform/
```

## What GitHub Actions does on push
1. Lint check (Black + Ruff)
2. Run full ETL pipeline
3. Validate database row counts
4. Deploy dashboard to GitHub Pages
5. Scheduled weekly re-run (Sunday 6am UTC)
