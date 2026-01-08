# Create a deploy branch and copy minimal requirements for Streamlit
# Run this from the repository root in PowerShell.

Set-StrictMode -Version Latest
$branch = "deploy"

# Ensure we're in a git repo
git rev-parse --is-inside-work-tree > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Not a git repository. Initialize git or run from the project root."
    exit 1
}

# Create and switch to branch
git checkout -b $branch
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to create branch $branch. Perhaps it already exists locally. Try: git checkout $branch"
    exit 1
}

# Copy minimal requirements
if (Test-Path "requirements-streamlit.txt") {
    Copy-Item -Path "requirements-streamlit.txt" -Destination "requirements.txt" -Force
    git add requirements.txt
    git commit -m "chore(deploy): use minimal requirements for Streamlit hosting" || Write-Host "No changes to commit."
    Write-Host "Created branch '$branch' and committed a minimal requirements.txt."
    Write-Host "To publish the branch run: git push -u origin $branch"
} else {
    Write-Error "requirements-streamlit.txt not found. Create it or check the file name."
    exit 1
}
