#!/usr/bin/env python3
"""
@Author: Zachary Thomas

Notes:
1. Requires remote DB to be accessible through the IP in settings.yaml. The DB is considered reachable if the IP without the port responds to the 
'ping' command.
2. Safely exit this script with ctrl-c
3. If the script exits with an error or unexpected shut down, the script should be restarted with 'python3 main.py forceOn'.
It's setup this way so that multiple instances of this script won't create doulbe entrys in the local database.
"""

import pickle
import time
import os
import sys
import traceback

from yaml import safe_load
from datetime import datetime, timedelta, timezone

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.query_api import QueryApi  # Used only for syntax highlighting
from influxdb_client.client.write_api import WriteApi  # Used only for syntax highlighting

def main(arg = None):

    if arg == "forceOn":
        scriptIsRunning = False
        with open("isRunning.pickle", "wb") as f:
            pickle.dump(scriptIsRunning, f)

    ### Check if this is the only instance running to avoid double mirroring ###
    with open("isRunning.pickle", "rb") as f:
        isRunning = pickle.load(f)
    
    if isRunning: # exit program
        print("This program is already running on this device, or exited due to an error. It can be forceably started \nregardless of other instances with 'python3 main.py forceOn'.")
        return
    
    isRunning = True
    with open("isRunning.pickle", "wb") as f:
        pickle.dump(isRunning, f)
    
    ### Enter steady state functionality ###
    with open("settings.yaml", "r") as f:
            settings = safe_load(f)

    localClient = InfluxDBClient(url=settings['LOCAL_IP'],
                                token=settings['LOCAL_TOKEN'],
                                org=settings['LOCAL_ORG'])
    localClientQuery = localClient.query_api()
    localClientWrite = localClient.write_api(write_options=SYNCHRONOUS)
    remoteClient = InfluxDBClient(url=settings["REMOTE_IP"],
                                  token=settings["REMOTE_TOKEN"],
                                  org=settings["REMOTE_ORG"])
    remoteClientQuery = remoteClient.query_api()

    logger(localClientWrite, settings, "DEBUG", "Started mirror service.")
    mainLoop(localClientQuery, localClientWrite, remoteClientQuery)

def mainLoop(localClientQuery: QueryApi, localClientWrite: WriteApi, remoteClientQuery: QueryApi):

    while True:
        
        # Use a try except block to handle the following:
        # 1. KeyboardInterrupt shutdown request
        # 2. Network errors caused by a loss of connection after pinging server and before 
        #    the mirror action has been completed

        try:
            ### Load settings ###
            with open("settings.yaml", "r") as f:
                settings = safe_load(f)
            
            performMirror(settings, localClientQuery, localClientWrite, remoteClientQuery)

            wait(settings, localClientWrite)

        except KeyboardInterrupt: #1
            scriptIsRunning = False
            with open("isRunning.pickle", "wb") as f:
                pickle.dump(scriptIsRunning, f)

            logger(localClientWrite, settings, "DEBUG", "Mirror service shutdown manually.")
            print("Mirror service shutdown.")
            
            return
        
        except Exception as e: #2
            tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
            print("".join(tb_str))
            logger(localClientWrite, settings, "ERROR", f"Python error occured: {e}")
            wait(settings, localClientWrite)


def logger(logWriter: WriteApi, settings: dict, level: str, message: str):
    """ Input: 
                WriterApi object setup for the local server

                settings: Dictionary of settings from the yaml
                
                level: A string representing the importance/type of the log.
                       Reccomended values: 'DEBUG', 'WARNING', and 'ERROR'. 
                       Will work with any string value, but it would be nice 
                       to continue using an all caps convention.

                message: The log message 
        
        Output:
                Writes the specific log to the "Logging" bucket in the database associated with the
                logWriter variable. All entries are under the measurement name "Logs"
                
                """
    
    if type(level) != str or type(message) != str:
        level = str(level)
        message = str(level)
    
    p = Point("Logs").tag("LOG_LEVEL", level).field("Message", message).time(time=datetime.now(tz=timezone.utc))
    logWriter.write("Logging", settings['LOCAL_ORG'], record=p)

def performMirror(settings, localClientQuery: QueryApi, localClientWrite: WriteApi, remoteClientQuery: QueryApi):

    ### See if remote database is available ###
    remoteIP = settings["REMOTE_IP"]

    ## Send 1 packet to the remote IP with the port removed and check if we get a resposne ##
    portIndex = remoteIP.find(':')
    response = os.system(f"ping -c 1 {remoteIP[:portIndex]}")

    if response == 0:
        logger(localClientWrite, settings, "DEBUG", "Successfully pinged the remote database.")
    else:
        logger(localClientWrite, settings, "DEBUG", "Unable to ping the remote database.")
        return 

    ### Mirror data we dont already have on a per-bucket basis ###
    for bucketName in settings["BUCKETS"]:
        # Log what we are doing to the Logging bucket
        logger(localClientWrite, settings, "DEBUG", f"Starting mirror of bucket: {bucketName}")
        print("=============== " + bucketName + "   ================")
        ## Find the most recent data we have on the local machine
        # The flux langauge queries seem to require a time specification with the flux range() function. For our application this 
        # causes problems because we don't know the timestamp of the last data point we stored (and it's not gauranteed that
        # it will be the same time as when we last pulled). The solution is to use multiple queries and increment to larger time values
        # for speed so that we don't have to query the entire local database to figure out which data we need to pull from the remote one.
        fluxTimes = ["-1m", "-1h", "-6h", "-12h", "-1d", "-7d", "-14d"] # Times in the past in flux notation
        gotData = False
        for fluxTime in fluxTimes:
            responseCSVIterator = localClientQuery.query_csv(f'from(bucket:"{bucketName}") |> range(start: {fluxTime}) |> sort(columns: ["_time"]) |> last()')

            for resp in responseCSVIterator: # Only enters loop if the iterator has data

                if gotData:
                    timeStamp = resp[timeIdx]
                    break

                for i, entry in enumerate(resp):
                    if entry == "_time":
                        timeIdx = i
                        gotData = True
                        break
                
            if gotData:
                break

        # If the local database has never mirrored before or if hard drives have been cycled use the recovery date
        if gotData == False:
            timeStamp = settings['RECOVER_DATA_SINCE_DATE']
    
        ## Request data time stamped after the most recent local data for this bucket ##
        print("Querying from: ", timeStamp)
        dataCSVIterator = remoteClientQuery.query_csv(f'from(bucket:"{bucketName}") |> range(start: {timeStamp})')

        ## #Push each data point to the local client ###

        # Parse the query csv rows into writeable point objects
        needHeaders = True
        customTags = []
        points=[]
        for resp in dataCSVIterator:
            # TODO: use these records to improve mirroring, skip for now
            if "#datatype" in resp or "#group" in resp or "#default" in resp:
                continue
            if not needHeaders and "result" in resp:
                continue
            if needHeaders and ('_value' in resp):
                needHeaders = False
                customTags = {} # key: index
                for i, header in enumerate(resp):
                    if header == '':
                        emptyIdx = i
                    elif header == 'result':
                        resultIdx = i
                    elif header == 'table':
                        tableIdx = i
                    elif header == '_start':
                        startIdx = i
                    elif header == '_stop':
                        stopIdx = i
                    elif header == '_time':
                        timeIdx = i
                    elif header == '_value':
                        valueIdx = i
                    elif header == '_field':
                        fieldIdx = i
                    elif header == '_measurement':
                        measurementIdx = i
                    else:
                        customTags[header] = i
            elif needHeaders == False:
                # Check data type of resp[valueIdx]
                try:
                    val = float(resp[valueIdx])
                except ValueError:
                    val = resp[valueIdx]
                pointToMirror = Point(resp[measurementIdx]).field(resp[fieldIdx], val).time(resp[timeIdx])
                for tagName, tagIdx in zip(customTags.keys(), customTags.values()):
                    pointToMirror.tag(tagName, resp[tagIdx])
                
                points += [pointToMirror]
                
        ### Actually mirror the point to local ###
        print("Mirroring ", len(points), " data points")
        localClientWrite.write(bucketName, settings["LOCAL_ORG"], record=points)
        logger(localClientWrite, settings, "DEBUG", f"Finished mirroring {len(points)} data points in the bucket: {bucketName}")

def wait(settings, localClientWrite):
    ### Wait the requested timeout period ###
    print("Waiting for ", settings['REFRESH_RATE'], " before trying to mirror.")
    logger(localClientWrite, settings, "DEBUG", f"Waiting for {settings['REFRESH_RATE']} before trying to mirror.")
    refreshRate = datetime.strptime(settings['REFRESH_RATE'], "%H:%M:%S")
    timeToCheckRemoteDB = datetime.now() + timedelta(hours=refreshRate.hour, minutes=refreshRate.minute, seconds=refreshRate.second)
    while True:
        if timeToCheckRemoteDB <= datetime.now():
            break
        time.sleep(0.2)
    
if __name__ == "__main__":

    # Catch the use case where the isRunning.pickle file has the wrong values
    # do to a forced exit of this program (other than cntrl-c)
    if len(sys.argv) >= 2:
        arg = sys.argv[1]
    else:
        arg = None

    main(arg)
