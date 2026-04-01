# 🚀 Publish to GitHub — One-Time Setup

## Step 1: Create the GitHub repository

Go to **https://github.com/new** and create a new repo:
- **Name:** `sazerac-analytics-platform`
- **Description:** `End-to-end data analytics pipeline & BI dashboard — Sazerac Company`
- **Visibility:** Public (required for GitHub Pages)
- **Do NOT** initialize with README, .gitignore, or license (the repo already has these)

## Step 2: Push from your machine

Run these 3 commands in your terminal, replacing `YOUR_USERNAME`:

```bash
# Add the remote (do this once)
git remote add origin https://github.com/YOUR_USERNAME/sazerac-analytics-platform.git

# Push everything
git push -u origin main
```

## Step 3: Enable GitHub Pages

1. Go to your repo → **Settings** → **Pages**
2. Under **Source**, select **GitHub Actions**
3. The CI/CD workflow at `.github/workflows/pipeline.yml` will automatically:
   - Run the full ETL pipeline
   - Generate the dashboard
   - Deploy it to `https://YOUR_USERNAME.github.io/sazerac-analytics-platform/`

## Step 4: Run the pipeline locally

```bash
pip install -r requirements.txt
python run_pipeline.py
open dashboard/sazerac_dashboard.html
```

## What the GitHub Actions workflow does

| Job | Trigger | Steps |
|-----|---------|-------|
| `lint` | Every push | Black format check + Ruff linting |
| `pipeline` | After lint | Scrape → ETL → DB → Dashboard → Validate |
| `deploy` | Main branch only | Runs pipeline + deploys to GitHub Pages |
