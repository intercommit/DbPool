#!/bin/bash
JAVAPAR="-Duser.timezone=Europe/Amsterdam" 
for i in `ls ../lib/*.jar`; do
	CLASSPATH=$i:$CLASSPATH
done
CLASSPATH=.:../classes:$CLASSPATH
java $JAVAPAR -cp $CLASSPATH nl.intercommit.dbpool.RunMySQL "$@"
