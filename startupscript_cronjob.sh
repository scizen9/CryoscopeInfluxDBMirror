#!/bin/bash

# startupscript_cronjob.sh
#
# This script is meant to be run as a cron job to make sure
# that the mirroring script is running.
#
# Example crontab entry that runs this script every minute
# (modify for the correct directory):
# */1 * * * * $HOME/CryoscopeInfluxDBMirror/startupscript_cronjob.sh > /dev/null 2>&1
#
# DIRECTORIES Set Up, PLEASE EDIT:
mirrordir=$HOME/CryoscopeInfluxDBMirror
#
ret=`pgrep -f CryoscopeInfluxDBMirror | wc -l`
if [ $ret -lt 1 ]; then
      cd $mirrordir
      # Use the line below instead to enable screen logging
      # screen -L -dmS CryoscopeInfluxDBMirror python3 main.py forceOn
      screen -dmS CryoscopeInfluxDBMirror python3 ./main.py forceOn
fi
