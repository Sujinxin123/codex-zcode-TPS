@echo off
chcp 65001 >nul
title Codex ^& ZCode Real-Time TPS Floating Ball
cd /d "%~dp0"

if exist "D:\Anaconda\pythonw.exe" (
    start "" "D:\Anaconda\pythonw.exe" "%~dp0tps_floating_ball.py" %*
) else if exist "D:\Anaconda\python.exe" (
    start "" "D:\Anaconda\python.exe" "%~dp0tps_floating_ball.py" %*
) else (
    start "" pythonw "%~dp0tps_floating_ball.py" %*
)
