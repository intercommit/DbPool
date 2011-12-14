rem Can be used with the following:
rem set JLIBS=conf
rem for %%i in (lib\*.jar) do call addjlib.bat %%i
rem @echo on
rem java -cp %JLIBS% nl.intercommit.mario.onramp.exim.main.EximOnramp

set JLIBS=%JLIBS%;%1