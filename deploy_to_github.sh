#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════
# deploy_to_github.sh
# Sazerac Brand & Analytics Intelligence Platform
# One-command GitHub deployment with GitHub Pages
# ═══════════════════════════════════════════════════════════════════════════
#
# PREREQUISITES:
#   1. GitHub account (free at github.com)
#   2. GitHub CLI: brew install gh   OR   https://cli.github.com
#   3. Authenticate: gh auth login
#
# RUN:
#   chmod +x deploy_to_github.sh
#   ./deploy_to_github.sh
#
# ═══════════════════════════════════════════════════════════════════════════

set -e

REPO_NAME="sazerac-analytics-platform"
DESCRIPTION="End-to-end data engineering & analytics portfolio project — Python ETL, SQLite DW, Power BI-style dashboard"

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║   Sazerac Analytics Platform — GitHub Deploy                    ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# ── Check gh CLI ──────────────────────────────────────────────────────────
if ! command -v gh &> /dev/null; then
    echo "❌ GitHub CLI (gh) not found."
    echo "   Install: brew install gh   or   https://cli.github.com"
    exit 1
fi

# ── Check auth ────────────────────────────────────────────────────────────
if ! gh auth status &> /dev/null; then
    echo "⚠️  Not authenticated. Running: gh auth login"
    gh auth login
fi

echo "✓ GitHub CLI authenticated"
GH_USER=$(gh api user --jq '.login')
echo "  User: @${GH_USER}"
echo ""

# ── Create GitHub repo ────────────────────────────────────────────────────
echo "Creating GitHub repository: ${REPO_NAME}..."
gh repo create "$REPO_NAME" \
    --public \
    --description "$DESCRIPTION" \
    --homepage "https://${GH_USER}.github.io/${REPO_NAME}" \
    --add-readme=false \
    2>/dev/null || echo "  (repo may already exist — continuing)"

# ── Set remote ────────────────────────────────────────────────────────────
REMOTE_URL="https://github.com/${GH_USER}/${REPO_NAME}.git"
git remote remove origin 2>/dev/null || true
git remote add origin "$REMOTE_URL"
echo "✓ Remote set: $REMOTE_URL"

# ── Push code ─────────────────────────────────────────────────────────────
echo ""
echo "Pushing to GitHub..."
git push -u origin main --force
echo "✓ Code pushed"

# ── Enable GitHub Pages ───────────────────────────────────────────────────
echo ""
echo "Enabling GitHub Pages..."

# Create gh-pages branch with the dashboard
git checkout -b gh-pages 2>/dev/null || git checkout gh-pages
cp dashboard/sazerac_dashboard.html index.html
git add index.html
git commit -m "deploy: GitHub Pages deployment — Sazerac Analytics Dashboard" 2>/dev/null || true
git push origin gh-pages --force
git checkout main

echo "✓ GitHub Pages deployed"
echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  DEPLOYMENT COMPLETE                                             ║"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║                                                                  ║"
echo "║  📁 GitHub Repo:                                                 ║"
printf "║     https://github.com/%-40s║\n" "${GH_USER}/${REPO_NAME}"
echo "║                                                                  ║"
echo "║  🌐 Live Dashboard (GitHub Pages):                               ║"
printf "║     https://%-51s║\n" "${GH_USER}.github.io/${REPO_NAME}"
echo "║                                                                  ║"
echo "║  ⏱  Pages may take 1-3 mins to go live after first deploy       ║"
echo "║                                                                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# ── Open browser ─────────────────────────────────────────────────────────
echo "Opening GitHub repository..."
gh repo view --web "$REPO_NAME" 2>/dev/null || true
