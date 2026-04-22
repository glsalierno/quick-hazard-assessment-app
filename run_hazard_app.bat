@echo off
REM Quick Hazard Assessment — Streamlit on http://localhost:8501
cd /d "%~dp0"
title Quick Hazard Assessment
echo.
echo Starting Quick Hazard Assessment...
echo Browser: http://localhost:8501
echo Close this window to stop the server (Ctrl+C first).
echo.
python -m streamlit run app.py --server.headless true 2>nul
if errorlevel 1 (
  py -3 -m streamlit run app.py --server.headless true 2>nul
)
if errorlevel 1 (
  echo Could not run Streamlit. Activate your venv and: pip install -r requirements.txt
  pause
  exit /b 1
)
pause
