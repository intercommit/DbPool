@echo off
set JLIBS=.;..\classes
for %%i in (..\lib\*.jar) do call addjlib.bat %%i
@echo on
