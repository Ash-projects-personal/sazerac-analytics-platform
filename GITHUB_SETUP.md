# 🚀 GitHub Setup & Deployment Guide

## Push to GitHub (3 commands)

```bash
# 1. Create a new repo on github.com named: sazerac-analytics-platform
#    (public, no README, no .gitignore)

# 2. Add your remote and push
git remote add origin https://github.com/YOUR_USERNAME/sazerac-analytics-platform.git
git push -u origin main

# 3. Done — GitHub Actions runs automatically on push
```

## Enable GitHub Pages (live dashboard)

1. Go to **Settings → Pages** in your repo
2. Source: **GitHub Actions**
3. Your dashboard will be live at:
   `https://YOUR_USERNAME.github.io/sazerac-analytics-platform/`

## What the CI/CD Pipeline Does

On every push to `main`, GitHub Actions:
1. **Lints** — runs `black` and `ruff` on all Python files
2. **Runs pipeline** — scrapes, processes, builds DB, generates dashboard
3. **Validates** — checks all 4 warehouse tables have rows
4. **Deploys** — publishes dashboard to GitHub Pages automatically

See `.github/workflows/pipeline.yml` for full spec.

## Quick verify after push

```bash
# Check Actions tab at: https://github.com/YOUR_USERNAME/sazerac-analytics-platform/actions
# Green checkmarks = all good
```
