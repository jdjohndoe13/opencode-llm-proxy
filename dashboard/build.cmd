@echo off
rem Build the dashboard in Docker and extract dist/ to the host.
rem Run from the dashboard/ directory.
setlocal
set NAME=llm-proxy-dashboard
set TMP=tmp-llm-dash
docker build -t %NAME% . || exit /b 1
if exist dist rmdir /s /q dist
docker rm -f %TMP% >nul 2>nul
docker create --name %TMP% %NAME% >nul || exit /b 1
docker cp %TMP%:/app/dist ./dist || exit /b 1
docker rm -f %TMP% >nul
echo.
echo OK: dashboard\dist generated.
