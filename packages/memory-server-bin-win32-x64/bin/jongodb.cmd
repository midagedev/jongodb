@echo off
set SCRIPT_DIR=%~dp0
set EXE_PATH=%SCRIPT_DIR%jongodb.exe
if not exist "%EXE_PATH%" (
  echo Missing jongodb.exe in %SCRIPT_DIR% 1>&2
  exit /b 1
)
"%EXE_PATH%" %*
