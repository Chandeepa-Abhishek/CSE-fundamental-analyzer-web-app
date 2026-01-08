# Deploying the CSE Stock Research Dashboard to Streamlit Community Cloud

This guide shows how to prepare and deploy the Streamlit dashboard to Streamlit Community Cloud.

Prerequisites
- A GitHub account
- Repository pushed to GitHub (public)

Steps
1. Option A: Push `main` (keep full repo)
   - Streamlit will try to install everything listed in `requirements.txt`. If you have heavy system deps (Camelot, Tabula), the build may fail.

2. Option B (recommended): Use a `deploy` branch with minimal requirements
   - Run the PowerShell script to create a `deploy` branch and copy `requirements-streamlit.txt` to `requirements.txt`:

```powershell
.
cd "d:\Projects\CODING\stock reasearch 2"
.\scripts\create_deploy_branch.ps1
git push -u origin deploy
```

3. In Streamlit Community Cloud:
   - Click "New app" → Connect GitHub → Select your repo and branch (e.g., `deploy`)
   - Set the main file to `web/app.py` and deploy

Notes
- If your dashboard requires processed data, ensure `data/processed/cse_companies_latest.csv` is committed or fetched remotely.
- For scheduled scraping, use GitHub Actions on the main branch to update `data/processed/` and commit.

Optional: I can create the `deploy` branch and push it to GitHub for you if you give me the repo URL and permission to push (or you can run the script locally).