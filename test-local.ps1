# Quick Local Test Script
# Opens the frontend in your default browser for local testing

Write-Host "🧪 Nimbus Insights - Local Test" -ForegroundColor Cyan
Write-Host "===============================" -ForegroundColor Cyan
Write-Host ""

# Check if config is set
$config = Get-Content -Path "frontend/config.js" -Raw
if ($config -match 'apiBaseUrl:\s*""') {
    Write-Host "⚠️  Warning: API URL not configured in frontend/config.js" -ForegroundColor Yellow
    Write-Host "   Deploy backend first with: .\deploy.ps1" -ForegroundColor Yellow
    Write-Host ""
}

# Get the full path to index.html
$indexPath = Resolve-Path "frontend/index.html"

Write-Host "🌐 Opening frontend in browser..." -ForegroundColor Green
Write-Host "   File: $indexPath" -ForegroundColor White
Write-Host ""
Write-Host "📝 Test with sample files:" -ForegroundColor Cyan
Write-Host "   ✅ samples/valid.csv" -ForegroundColor Green
Write-Host "   ⚠️  samples/missing_columns.csv" -ForegroundColor Yellow
Write-Host "   ❌ samples/malformed.csv" -ForegroundColor Red
Write-Host ""

Start-Process $indexPath
