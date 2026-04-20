@echo off
REM Simple batch file wrapper for deployment (for users without PowerShell execution policy)
REM Usage: deploy.bat

echo.
echo ========================================
echo  Nimbus Insights - Simple Deployment
echo ========================================
echo.

REM Check if PowerShell is available
where powershell >nul 2>nul
if %errorlevel% neq 0 (
    echo ERROR: PowerShell not found!
    echo Please install PowerShell to continue.
    pause
    exit /b 1
)

echo Running deployment script...
echo.

powershell -ExecutionPolicy Bypass -File "%~dp0deploy.ps1" %*

if %errorlevel% neq 0 (
    echo.
    echo Deployment failed!
    pause
    exit /b 1
)

echo.
echo Deployment completed successfully!
echo.
pause
