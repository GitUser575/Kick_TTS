@echo off
cd /d "%~dp0"
TITLE Chatterbox Turbo Setup
color 0B
echo ===================================================
echo     Chatterbox Turbo TTS - Hardware Configurator
echo ===================================================
echo.

REM ======================================================================
REM --- Step 1: Detect Installed Python Version (3.10 - 3.13) & Fix PATH ---
REM ======================================================================
echo Checking for Python installation...
set "PY_VER="
set "PYTHON_CMD="
set "PY_DIR="

REM 1. Check if 'python' in active PATH works
python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" >nul 2>&1
if not errorlevel 1 (
    for /f "tokens=*" %%V in ('python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2^>nul') do set "PY_VER=%%V"
    for /f "tokens=*" %%D in ('python -c "import sys, os; print(os.path.dirname(sys.executable))" 2^>nul') do set "PY_DIR=%%D"
    set "PYTHON_CMD=python"
)

REM 2. Check if 'py' launcher is present
if "%PY_VER%"=="" (
    py -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" >nul 2>&1
    if not errorlevel 1 (
        for /f "tokens=*" %%V in ('py -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2^>nul') do set "PY_VER=%%V"
        for /f "tokens=*" %%D in ('py -c "import sys, os; print(os.path.dirname(sys.executable))" 2^>nul') do set "PY_DIR=%%D"
        set "PYTHON_CMD=py"
    )
)

REM 3. If Python was not in active PATH, scan Windows Registry and common install paths
if "%PY_VER%"=="" (
    for %%V in (3.13 3.12 3.11 3.10) do (
        if "%PY_DIR%"=="" (
            for /f "tokens=2*" %%A in ('reg query "HKCU\Software\Python\PythonCore\%%V\InstallPath" /ve 2^>nul') do (
                if exist "%%B\python.exe" (
                    set "PY_DIR=%%B"
                    set "PY_VER=%%V"
                )
            )
        )
        if "%PY_DIR%"=="" (
            for /f "tokens=2*" %%A in ('reg query "HKLM\Software\Python\PythonCore\%%V\InstallPath" /ve 2^>nul') do (
                if exist "%%B\python.exe" (
                    set "PY_DIR=%%B"
                    set "PY_VER=%%V"
                )
            )
        )
        if "%PY_DIR%"=="" (
            set "SHORT_V=%%V"
            set "SHORT_V=!SHORT_V:.=!"
            if exist "%LocalAppData%\Programs\Python\Python!SHORT_V!\python.exe" (
                set "PY_DIR=%LocalAppData%\Programs\Python\Python!SHORT_V!"
                set "PY_VER=%%V"
            )
            if exist "%ProgramFiles%\Python!SHORT_V!\python.exe" (
                set "PY_DIR=%ProgramFiles%\Python!SHORT_V!"
                set "PY_VER=%%V"
            )
        )
    )
)

REM 4. If Python was found installed on the machine, ensure PATH is permanently enabled!
if not "%PY_DIR%"=="" (
    if "%PY_DIR:~-1%"=="\" set "PY_DIR=%PY_DIR:~0,-1%"
    echo [INFO] Detected existing Python installation at: %PY_DIR%
    echo [INFO] Enabling Python and Scripts in Windows PATH...
    set "PATH=%PY_DIR%;%PY_DIR%\Scripts;%PATH%"
    set "PYTHON_CMD=python"
    
    REM Permanently update Windows User PATH via PowerShell without 1024-character truncation
    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
        "$userPath = [Environment]::GetEnvironmentVariable('Path', 'User'); " ^
        "$pyPath = '%PY_DIR%'; $scriptsPath = '%PY_DIR%\Scripts'; " ^
        "if ($userPath -notlike ('*' + $pyPath + '*')) { " ^
        "    $newPath = $pyPath + ';' + $scriptsPath + ';' + $userPath; " ^
        "    [Environment]::SetEnvironmentVariable('Path', $newPath, 'User'); " ^
        "    Write-Host '[SUCCESS] Python added to permanent Windows User PATH.' " ^
        "}" >nul 2>&1
)

set "PY_SUPPORTED=0"
if "%PY_VER%"=="3.10" set "PY_SUPPORTED=1"
if "%PY_VER%"=="3.11" set "PY_SUPPORTED=1"
if "%PY_VER%"=="3.12" set "PY_SUPPORTED=1"
if "%PY_VER%"=="3.13" set "PY_SUPPORTED=1"

if "%PY_SUPPORTED%"=="1" (
    echo [SUCCESS] Supported Python version detected: %PY_VER%
    goto SetPythonTags
)

REM ======================================================================
REM --- Step 2: Auto-Install Python 3.11.9 if no supported version found ---
REM ======================================================================
echo ======================================================================
if "%PY_VER%"=="" (
    echo  No compatible Python installation was detected on your PC.
) else (
    echo  Detected Python %PY_VER%, which is outside supported versions (3.10 - 3.13).
)
echo  Defaulting to automatically installing Python version 3.11.9 (64-bit)...
echo ======================================================================
echo.

set "PY_INSTALLER=%TEMP%\python-3.11.9-amd64.exe"
set "PY_URL=https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe"

echo [1/3] Downloading Python 3.11.9 installer...
curl -L -o "%PY_INSTALLER%" "%PY_URL%" >nul 2>&1
if errorlevel 1 (
    echo [INFO] Curl unavailable, downloading via PowerShell...
    powershell -NoProfile -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; (New-Object Net.WebClient).DownloadFile('%PY_URL%', '%PY_INSTALLER%')"
)

if not exist "%PY_INSTALLER%" (
    color 0C
    echo ======================================================================
    echo  [ERROR: FAILED TO DOWNLOAD PYTHON 3.11.9]
    echo ======================================================================
    echo Could not download the official Python installer automatically.
    echo Please manually download and install Python 3.11.9 from:
    echo %PY_URL%
    echo.
    echo CRITICAL: Ensure you check "Add Python.exe to PATH" during installation!
    echo ======================================================================
    pause
    exit /b 1
)

echo [2/3] Installing Python 3.11.9 (Adding to PATH, pip included)...
echo (Please allow Windows permissions if User Account Control prompts you)
start /wait "" "%PY_INSTALLER%" /passive InstallAllUsers=0 PrependPath=1 Include_pip=1 Include_launcher=1 Shortcuts=0

if exist "%PY_INSTALLER%" del "%PY_INSTALLER%" >nul 2>&1

echo [3/3] Refreshing system PATH...
for /f "tokens=2*" %%A in ('reg query "HKCU\Environment" /v Path 2^>nul') do set "USER_PATH=%%B"
for /f "tokens=2*" %%A in ('reg query "HKLM\System\CurrentControlSet\Control\Session Manager\Environment" /v Path 2^>nul') do set "SYS_PATH=%%B"
set "PATH=%USER_PATH%;%SYS_PATH%;%PATH%;%LocalAppData%\Programs\Python\Python311;%LocalAppData%\Programs\Python\Python311\Scripts"

REM Verify Python is accessible
set "PY_VER="
python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" >nul 2>&1
if not errorlevel 1 (
    for /f "tokens=*" %%V in ('python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2^>nul') do (
        set "PY_VER=%%V"
    )
)
if "%PY_VER%"=="" (
    for /f "tokens=*" %%V in ('py -3.11 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2^>nul') do (
        set "PY_VER=%%V"
        set "PYTHON_CMD=py -3.11"
    )
)

if "%PY_VER%"=="" (
    color 0A
    echo ======================================================================
    echo  Python 3.11.9 has been successfully installed!
    echo.
    echo  To ensure your command prompt recognizes the newly installed Python,
    echo  please close this window and run setup.bat again to continue setup.
    echo ======================================================================
    pause
    exit /b 0
)

:SetPythonTags
if "%PY_VER%"=="3.10" set "CP_TAG=cp310-cp310"
if "%PY_VER%"=="3.11" set "CP_TAG=cp311-cp311"
if "%PY_VER%"=="3.12" set "CP_TAG=cp312-cp312"
if "%PY_VER%"=="3.13" set "CP_TAG=cp313-cp313"

REM ======================================================================
REM --- Step 3: Check / Auto-Install Git (Portable MinGit) ---
REM ======================================================================
echo.
echo Checking for Git...
set "MINGIT_DIR=%~dp0bin\git"
if exist "%MINGIT_DIR%\cmd\git.exe" (
    set "PATH=%MINGIT_DIR%\cmd;%PATH%"
)

git --version >nul 2>&1
if not errorlevel 1 (
    for /f "tokens=*" %%G in ('git --version 2^>nul') do echo [SUCCESS] %%G detected.
    goto CheckVS
)

echo ======================================================================
echo  Git is not detected on your PC.
echo  Automatically downloading and configuring portable MinGit...
echo ======================================================================
echo.

if not exist "%~dp0bin" mkdir "%~dp0bin" 2>nul
if not exist "%MINGIT_DIR%" mkdir "%MINGIT_DIR%" 2>nul

set "MINGIT_ZIP=%TEMP%\MinGit-portable.zip"
echo [1/3] Downloading portable MinGit package (~35 MB)...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; " ^
    "$url = 'https://github.com/git-for-windows/git/releases/download/v2.55.0.windows.5/MinGit-2.55.0.5-64-bit.zip'; " ^
    "try { (New-Object Net.WebClient).DownloadFile($url, '%MINGIT_ZIP%') } " ^
    "catch { $latest = (Invoke-RestMethod 'https://api.github.com/repos/git-for-windows/git/releases/latest').assets | Where-Object { $_.name -like '*MinGit*64-bit.zip' -and $_.name -notlike '*busybox*' } | Select-Object -First 1; (New-Object Net.WebClient).DownloadFile($latest.browser_download_url, '%MINGIT_ZIP%') }"

if not exist "%MINGIT_ZIP%" (
    curl -L -o "%MINGIT_ZIP%" "https://github.com/git-for-windows/git/releases/download/v2.55.0.windows.5/MinGit-2.55.0.5-64-bit.zip" >nul 2>&1
)

if not exist "%MINGIT_ZIP%" (
    color 0C
    echo ======================================================================
    echo  [ERROR: FAILED TO DOWNLOAD MINGIT]
    echo ======================================================================
    echo Could not download portable MinGit automatically.
    echo Please install Git for Windows manually from: https://git-scm.com/download/win
    echo ======================================================================
    pause
    exit /b 1
)

echo [2/3] Extracting portable MinGit...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -Path '%MINGIT_ZIP%' -DestinationPath '%MINGIT_DIR%' -Force"
if exist "%MINGIT_ZIP%" del "%MINGIT_ZIP%" >nul 2>&1

echo [3/3] Registering MinGit in active session...
set "PATH=%MINGIT_DIR%\cmd;%PATH%"

git --version >nul 2>&1
if errorlevel 1 (
    color 0C
    echo [ERROR] MinGit could not be initialized. Please install Git manually.
    pause
    exit /b 1
)
echo [SUCCESS] Portable MinGit configured successfully!

:CheckVS
REM ======================================================================
REM --- Step 4: Visual Studio C++ Build Tools & Windows SDK ---
REM ======================================================================
echo.
echo Checking for Visual Studio C++ Build Tools and Windows SDK...
set "VSWHERE=%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe"
set "HAS_VC_TOOLS=0"
set "HAS_WIN_SDK=0"
set "VS_INSTALL_PATH="

if exist "%VSWHERE%" (
    REM Check if C++ compiler tools (x86/x64) are installed
    "%VSWHERE%" -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 >nul 2>&1
    if not errorlevel 1 set "HAS_VC_TOOLS=1"

    REM Check if Windows 10/11 SDK is installed
    for /f "usebackq tokens=*" %%A in (`"%VSWHERE%" -products * -requires Microsoft.VisualStudio.Component.Windows10SDK 2^>nul`) do set "HAS_WIN_SDK=1"
    if "%HAS_WIN_SDK%"=="0" (
        for /f "usebackq tokens=*" %%A in (`"%VSWHERE%" -products * -requires Microsoft.VisualStudio.Component.Windows11SDK 2^>nul`) do set "HAS_WIN_SDK=1"
    )

    REM Get latest Visual Studio installation path if present
    for /f "usebackq tokens=*" %%I in (`"%VSWHERE%" -products * -latest -property installationPath 2^>nul`) do (
        set "VS_INSTALL_PATH=%%I"
    )
)

if "%HAS_VC_TOOLS%"=="1" if "%HAS_WIN_SDK%"=="1" (
    echo [SUCCESS] Visual Studio C++ Build Tools and Windows SDK detected!
    goto CheckGPU
)

if not "%VS_INSTALL_PATH%"=="" (
    echo ======================================================================
    echo  Visual Studio is installed, but requires the "Desktop development with C++"
    echo  workload and "Windows SDK" component.
    echo  Updating your existing Visual Studio installation...
    echo ======================================================================
    echo.
    echo (A Visual Studio installer window will show the component installation progress)
    "%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\setup.exe" modify --installPath "%VS_INSTALL_PATH%" --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended --passive --norestart
    echo [SUCCESS] Visual Studio components updated!
    goto CheckGPU
)

echo ======================================================================
echo  Visual Studio C++ Build Tools not detected.
echo  Downloading official Microsoft Visual Studio Build Tools installer...
echo ======================================================================
echo.
set "VS_INSTALLER=%TEMP%\vs_BuildTools.exe"
set "VS_URL=https://aka.ms/vs/17/release/vs_BuildTools.exe"

echo [1/2] Downloading vs_BuildTools.exe (~2 MB bootstrapper)...
curl -L -o "%VS_INSTALLER%" "%VS_URL%" >nul 2>&1
if errorlevel 1 (
    powershell -NoProfile -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; (New-Object Net.WebClient).DownloadFile('%VS_URL%', '%VS_INSTALLER%')"
)

if not exist "%VS_INSTALLER%" (
    color 0C
    echo ======================================================================
    echo  [ERROR: FAILED TO DOWNLOAD VS BUILD TOOLS]
    echo ======================================================================
    echo Could not download vs_BuildTools.exe automatically.
    echo Please manually download and install from: %VS_URL%
    echo Make sure to select "Desktop development with C++" and "Windows SDK".
    echo ======================================================================
    pause
    exit /b 1
)

echo [2/2] Installing Desktop development with C++ and Windows SDK...
echo (Please click 'Yes' on the Windows Administrator UAC prompt)
echo (The Visual Studio Installer will display the download and installation progress)
start /wait "" "%VS_INSTALLER%" --passive --wait --norestart --nocache --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended

if exist "%VS_INSTALLER%" del "%VS_INSTALLER%" >nul 2>&1
echo [SUCCESS] Visual Studio C++ Build Tools and Windows SDK installed!

:CheckGPU

REM ======================================================================
REM --- Step 4: Detect and Validate NVIDIA GPU (10/20/30/40/50 Series) ---
REM ======================================================================
echo.
echo Detecting NVIDIA GPU...
set "GPU_NAME="

REM 1. Query NVIDIA-SMI (Native Driver Tool)
for /f "tokens=*" %%A in ('nvidia-smi --query-gpu^=name --format^=csv^,noheader 2^>nul') do (
    set "GPU_NAME=%%A"
)

REM 2. Query PowerShell CIM (Windows 11 / modern Windows)
if "%GPU_NAME%"=="" (
    for /f "usebackq delims=" %%A in (`powershell -NoProfile -Command "Get-CimInstance Win32_VideoController | Where-Object { $_.Name -match 'NVIDIA|GeForce|TITAN|Quadro|RTX|GTX' } | Select-Object -ExpandProperty Name" 2^>nul`) do (
        set "GPU_NAME=%%A"
    )
)

REM 3. Query WMIC (Legacy Windows 10 Fallback)
if "%GPU_NAME%"=="" (
    for /f "tokens=2* delims==" %%A in ('wmic path win32_VideoController get name /value ^| find /I "NVIDIA" 2^>nul') do (
        set "GPU_NAME=%%A"
    )
)

REM If no GPU was detected at all, halt immediately
if "%GPU_NAME%"=="" goto UnsupportedGPU

REM Validate that the detected video card is an NVIDIA GPU
echo %GPU_NAME% | findstr /I "NVIDIA GeForce TITAN Quadro RTX GTX" >nul
if errorlevel 1 goto UnsupportedGPU

REM 1. Check for RTX 50-Series (Blackwell)
echo %GPU_NAME% | findstr /I "5050 5060 5070 5080 5090" >nul
if not errorlevel 1 goto ConfigBlackwell

REM 2. Check for GTX 10-Series (Pascal)
echo %GPU_NAME% | findstr /I "1030 1050 1060 1070 1080 Titan\ Xp" >nul
if not errorlevel 1 goto ConfigPascal

REM 3. Check for RTX 20/30/40-Series (Turing, Ampere, Ada Lovelace)
echo %GPU_NAME% | findstr /I "2050 2060 2070 2080 3050 3060 3070 3080 3090 4050 4060 4070 4080 4090 Titan\ RTX 1650 1660" >nul
if not errorlevel 1 goto ConfigModern

REM Also support professional RTX workstation cards (A2000-A6000, RTX 2000-6000 Ada)
echo %GPU_NAME% | findstr /I "A2000 A3000 A4000 A4500 A5000 A5500 A6000 RTX\ 2000 RTX\ 3000 RTX\ 4000 RTX\ 4500 RTX\ 5000 RTX\ 6000" >nul
if not errorlevel 1 goto ConfigModern

REM If none of the 10, 20, 30, 40, or 50 series matched (e.g. GTX 980, 970, 780, 750, or older), halt!
goto UnsupportedGPU

REM ======================================================================
REM --- Hardware Configuration Profiles ---
REM ======================================================================
:ConfigPascal
set "ARCH=Pascal (GTX 10-Series)"
set "TORCH_VER=2.6.0"
set "CUDA_TAG=cu118"
set "CUDA_URL=https://download.pytorch.org/whl/cu118"
set "TARGET_WHEEL=torch-2.6.0+cu118-%CP_TAG%-win_amd64.whl"
set "AUDIO_WHEEL=torchaudio-2.6.0+cu118-%CP_TAG%-win_amd64.whl"
goto PrintConfig

:ConfigModern
set "ARCH=Modern RTX (20/30/40-Series)"
set "TORCH_VER=2.6.0"
set "CUDA_TAG=cu124"
set "CUDA_URL=https://download.pytorch.org/whl/cu124"
set "TARGET_WHEEL=torch-2.6.0+cu124-%CP_TAG%-win_amd64.whl"
set "AUDIO_WHEEL=torchaudio-2.6.0+cu124-%CP_TAG%-win_amd64.whl"
goto PrintConfig

:ConfigBlackwell
set "ARCH=Blackwell (RTX 50-Series)"
set "TORCH_VER=2.9.1"
set "CUDA_TAG=cu128"
set "CUDA_URL=https://download.pytorch.org/whl/cu128"
set "TARGET_WHEEL=torch-2.9.1+cu128-%CP_TAG%-win_amd64.whl"
set "AUDIO_WHEEL=torchaudio-2.9.1+cu128-%CP_TAG%-win_amd64.whl"
goto PrintConfig

:UnsupportedGPU
color 0C
echo ======================================================================
echo  [INSTALLATION HALTED: REQUIRED NVIDIA GPU NOT DETECTED]
echo ======================================================================
if "%GPU_NAME%"=="" (
    echo No NVIDIA GPU was detected on this computer.
) else (
    echo Detected Graphics Card: %GPU_NAME%
)
echo.
echo Chatterbox Turbo strictly requires an NVIDIA GPU from one of the
echo following supported series to proceed with installation:
echo.
echo   * NVIDIA GeForce GTX 10-Series (e.g. GTX 1050, 1060, 1070, 1080)
echo   * NVIDIA GeForce RTX 20-Series (e.g. RTX 2060, 2070, 2080)
echo   * NVIDIA GeForce RTX 30-Series (e.g. RTX 3050, 3060, 3070, 3080, 3090)
echo   * NVIDIA GeForce RTX 40-Series (e.g. RTX 4050, 4060, 4070, 4080, 4090)
echo   * NVIDIA GeForce RTX 50-Series (e.g. RTX 5070, 5080, 5090)
echo.
echo You do not have one of the required video cards to continue with installation.
echo Non-NVIDIA graphics cards (AMD/Intel) and older NVIDIA generations 
echo (such as GTX 900-series or 700-series) are not supported.
echo.
echo Installation cannot continue.
echo ======================================================================
pause
exit /b 1

:PrintConfig
echo.
echo ===================================================
echo  HARDWARE ^& ENVIRONMENT CONFIGURATION:
echo ===================================================
echo  [SUCCESS] Python Version: %PY_VER% (%CP_TAG%)
echo  [SUCCESS] GPU Detected:   %GPU_NAME%
echo  [SUCCESS] Architecture:   %ARCH%
echo  [SUCCESS] PyTorch Wheel:  %TARGET_WHEEL%
echo  [SUCCESS] Audio Wheel:    %AUDIO_WHEEL%
echo ===================================================
echo.

REM ======================================================================
REM --- Step 5: Install PyTorch with GPU CUDA Acceleration ---
REM ======================================================================
python -c "import torch; exit(0 if torch.__version__ == '%TORCH_VER%+%CUDA_TAG%' and torch.cuda.is_available() else 1)" >nul 2>&1
if not errorlevel 1 (
    echo [INFO] PyTorch %TORCH_VER%+%CUDA_TAG% with CUDA acceleration is already installed. Skipping...
    goto InstallDeps
)

echo [INFO] Installing %TARGET_WHEEL% and %AUDIO_WHEEL% from %CUDA_URL%...
pip uninstall torch torchaudio torchvision -y >nul 2>&1
pip install torch==%TORCH_VER%+%CUDA_TAG% torchaudio==%TORCH_VER%+%CUDA_TAG% --index-url %CUDA_URL%
if errorlevel 1 (
    echo [INFO] Attempting direct wheel download from PyTorch repository...
    pip install "https://download.pytorch.org/whl/%CUDA_TAG%/%TARGET_WHEEL%" "https://download.pytorch.org/whl/%CUDA_TAG%/%AUDIO_WHEEL%"
)

:VerifyTorchInstall
python -c "import torch; exit(0 if torch.cuda.is_available() else 1)" >nul 2>&1
if errorlevel 1 (
    echo [WARNING] CUDA verification failed. Re-attempting installation from direct wheel URLs...
    pip uninstall torch torchaudio torchvision -y >nul 2>&1
    pip install "https://download.pytorch.org/whl/%CUDA_TAG%/%TARGET_WHEEL%" "https://download.pytorch.org/whl/%CUDA_TAG%/%AUDIO_WHEEL%"
)

REM ======================================================================
REM --- Step 6: Install Base Dependencies ---
REM ======================================================================
:InstallDeps
echo.
echo Installing Base Requirements...
pip install -r requirements.txt

REM ======================================================================
REM --- Step 7: Install Chatterbox Voice Cloning Engine ---
REM ======================================================================
echo.
echo Installing Neural Voice Cloning Engine (Chatterbox Turbo)...
echo (This may take a moment to compile audio dependencies...)

python -c "import chatterbox" >nul 2>&1
if not errorlevel 1 (
    echo [INFO] Chatterbox TTS is already installed. Skipping...
    goto VerifyTTS
)

if "%ARCH%"=="Blackwell (RTX 50-Series)" goto InstallChatterboxBlackwell

REM Modern and Pascal installation
pip install chatterbox-tts
if errorlevel 1 goto FallbackNoDeps
goto VerifyTTS

:InstallChatterboxBlackwell
pip install chatterbox-tts --no-deps
echo [INFO] Installing required Blackwell TTS dependencies...
pip install transformers accelerate tqdm scipy numpy peft torchcodec soundfile
goto VerifyTTS

:FallbackNoDeps
echo [WARNING] Standard Chatterbox install failed or modified PyTorch.
echo [INFO] Applying clean isolated installation method...
pip uninstall torch torchaudio torchvision chatterbox-tts -y >nul 2>&1
pip install torch==%TORCH_VER%+%CUDA_TAG% torchaudio==%TORCH_VER%+%CUDA_TAG% --index-url %CUDA_URL%
pip install chatterbox-tts --no-deps
pip install transformers accelerate tqdm scipy numpy peft torchcodec soundfile

:VerifyTTS
REM Ensure GPU acceleration was not accidentally downgraded to CPU
python -c "import torch; exit(0 if torch.cuda.is_available() else 1)" >nul 2>&1
if errorlevel 1 (
    echo [WARNING] GPU acceleration was downgraded by a dependency. Restoring %TARGET_WHEEL%...
    pip uninstall torch torchaudio torchvision -y >nul 2>&1
    pip install torch==%TORCH_VER%+%CUDA_TAG% torchaudio==%TORCH_VER%+%CUDA_TAG% --index-url %CUDA_URL%
)

REM ======================================================================
REM --- Step 8: Playwright Browser for KickBot Audio ---
REM ======================================================================
echo.
echo Installing headless browser for KickBot TTS audio listener...
playwright install chromium

echo.
echo ===================================================
echo Setup Complete! 
echo Python: %PY_VER% (%CP_TAG%)
echo GPU / Architecture: %ARCH%
echo PyTorch Wheel: %TARGET_WHEEL%
echo.
echo You can now close this window and double-click 
echo "Launch Chatterbox.bat" to start Chatterbox Turbo!
echo ===================================================
pause
