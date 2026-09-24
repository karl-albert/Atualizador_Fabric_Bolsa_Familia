@echo off
chcp 65001 > nul
title Atualizador Mensal Bolsa Família - Microsoft Fabric
color 0B

echo ================================================================================
echo   BOLSA FAMÍLIA ANALYTICS — ATUALIZADOR MENSAL MICROSOFT FABRIC
echo ================================================================================
echo.

python "%~dp0atualizador_mensal_fabric.py"

echo.
echo ================================================================================
echo   Processo finalizado. Pressione qualquer tecla para sair...
echo ================================================================================
pause > nul
