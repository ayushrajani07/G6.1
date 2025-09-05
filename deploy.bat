@echo off
ECHO 🚀 G6 Platform Deployment
ECHO ==========================

REM --- Check Python version ---
ECHO [INFO] Checking Python version...
python --version

REM --- Create virtual environment if it doesn't exist ---
IF NOT EXIST "venv\" (
    ECHO [INFO] Creating virtual environment...
    python -m venv venv
) ELSE (
    ECHO [INFO] Virtual environment already exists.
)

REM --- Activate virtual environment ---
ECHO [INFO] Activating virtual environment...
CALL venv\Scripts\activate.bat

REM --- Install/update dependencies ---
ECHO [INFO] Installing dependencies from requirements.txt...
pip install -r requirements.txt

REM --- Create necessary directories ---
ECHO [INFO] Creating data directories...
IF NOT EXIST "data\csv\overview" mkdir "data\csv\overview"
IF NOT EXIST "data\csv\options" mkdir "data\csv\options"
IF NOT EXIST "data\csv\overlay" mkdir "data\csv\overlay"
IF NOT EXIST ".cache" mkdir ".cache"
IF NOT EXIST "logs" mkdir "logs"

REM --- Copy environment template if .env doesn't exist ---
IF NOT EXIST ".env" (
    ECHO [INFO] Creating .env file from template...
    copy "src\config\environment.template" ".env" > NUL
    ECHO [!!IMPORTANT!!] Please edit the new .env file with your actual Kite Connect credentials!
) ELSE (
    ECHO [INFO] .env file already exists.
)

ECHO.
ECHO ✅ Deployment complete!
ECHO.
ECHO Next steps:
ECHO 1. Edit .env with your Kite API key and secret.
ECHO 2. Customize src/config/g6_config.json if needed.
ECHO 3. Run the platform using the new 'run.bat' script.
ECHO.
pause