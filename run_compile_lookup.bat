@echo off
REM Launch the Compile Hazard Lookup Streamlit page (GHaz6 + P2OASys/HSPiP lookup + TCI SDS).
cd /d "%~dp0"
python -m streamlit run pages/01_Compile_Hazard_Lookup.py
