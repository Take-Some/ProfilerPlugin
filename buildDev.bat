@echo off
setlocal EnableExtensions
call "%~dp0..\build_plugin.cmd" "%~dp0" "buildDev" %*
exit /b %ERRORLEVEL%
