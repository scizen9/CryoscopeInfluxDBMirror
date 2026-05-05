#!/usr/bin/env python3
"""
@Author: Zachary Thomas

Notes:
1. Requires remote DB to be accessible through the IP in settings.yaml.
    The DB is considered reachable if the IP without the port responds to the 'ping' command.
2. Safely exit this script with ctrl-c
3. If the script exits with an error or unexpected shut down, the script should be restarted with
    'python3 main.py forceOn'.
It's setup this way so that multiple instances of this script won't create doulbe entrys
    in the local database.
"""

import pickle
import time
import os
import sys
import traceback

from datetime import datetime, timedelta, timezone
from yaml import safe_load

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.query_api import QueryApi  # Used only for syntax highlighting
from influxdb_client.client.write_api import WriteApi  # Used only for syntax highlighting

def main(arg = None):
    """Main routine"""
    if arg == "forceOn":
        script_is_running = False
        with open("isRunning.pickle", "wb") as f:
            pickle.dump(script_is_running, f)

    ### Check if this is the only instance running to avoid double mirroring ###
    with open("isRunning.pickle", "rb") as run_fl:
        is_running = pickle.load(run_fl)

    if is_running: # exit program
        print("This program is already running on this device, or exited due to an error. "
              "It can be forceably started \nregardless of other instances "
              "with 'python3 main.py forceOn'.")
        return

    is_running = True
    with open("isRunning.pickle", "wb") as run_fl:
        pickle.dump(is_running, run_fl)

    ### Enter steady state functionality ###
    with open("settings.yaml", "r") as set_fl:
        settings = safe_load(set_fl)

    local_client = InfluxDBClient(url=settings['LOCAL_IP'],
                                token=settings['LOCAL_TOKEN'],
                                org=settings['LOCAL_ORG'])
    local_client_query = local_client.query_api()
    local_client_write = local_client.write_api(write_options=SYNCHRONOUS)
    remote_client = InfluxDBClient(url=settings["REMOTE_IP"],
                                  token=settings["REMOTE_TOKEN"],
                                  org=settings["REMOTE_ORG"])
    remote_client_query = remote_client.query_api()

    logger(local_client_write, settings, "DEBUG", "Started mirror service.")
    mainLoop(local_client_query, local_client_write, remote_client_query)

def mainLoop(localClientQuery: QueryApi, localClientWrite: WriteApi, remoteClientQuery: QueryApi):
    """Main loop"""
    while True:

        # Use a try except block to handle the following:
        # 1. KeyboardInterrupt shutdown request
        # 2. Network errors caused by a loss of connection after pinging server and before
        #    the mirror action has been completed

        try:
            ### Load settings ###
            with open("settings.yaml", "r") as set_fl:
                settings = safe_load(set_fl)

            performMirror(settings, localClientQuery, localClientWrite, remoteClientQuery)

            wait(settings, localClientWrite)

        except KeyboardInterrupt: #1
            script_is_running = False
            with open("isRunning.pickle", "wb") as run_fl:
                pickle.dump(script_is_running, run_fl)

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
                Recommended values: 'DEBUG', 'WARNING', and 'ERROR'.
                Will work with any string value, but it would be nice
                to continue using an all caps convention.

        message: The log message

        Output:
                Writes the specific log to the "Logging" bucket in the database associated with the
                logWriter variable. All entries are under the measurement name "Logs"

                """

    if not isinstance(level, str) or isinstance(message, str):
        level = str(level)
        message = str(level)

    p = Point("Logs").tag("LOG_LEVEL", level).field("Message", message).time(
        time=datetime.now(tz=timezone.utc))
    logWriter.write("Logging", settings['LOCAL_ORG'], record=p)

def performMirror(settings, localClientQuery: QueryApi, localClientWrite: WriteApi,
                  remoteClientQuery: QueryApi):

    ### See if remote database is available ###
    remote_ip = settings["REMOTE_IP"]

    ## Send 1 packet to the remote IP with the port removed and check if we get a resposne ##
    port_index = remote_ip.find(':')
    response = os.system(f"ping -c 1 {remote_ip[:port_index]}")

    if response == 0:
        logger(localClientWrite, settings, "DEBUG", "Successfully pinged the remote database.")
    else:
        logger(localClientWrite, settings, "DEBUG", "Unable to ping the remote database.")
        return

    ### Mirror data we don't already have on a per-bucket basis ###
    for bucket_name in settings["BUCKETS"]:
        # Log what we are doing to the Logging bucket
        logger(localClientWrite, settings, "DEBUG", f"Starting mirror of bucket: {bucket_name}")
        print("=============== " + bucket_name + "   ================")
        ## Find the most recent data we have on the local machine
        # The flux langauge queries seem to require a time specification with the flux range()
        # function. For our application this causes problems because we don't know the timestamp of
        # the last data point we stored (and it's not gauranteed that
        # it will be the same time as when we last pulled). The solution is to use multiple queries
        # and increment to larger time values for speed so that we don't have to query the entire
        # local database to figure out which data we need to pull from the remote one.

        # If the local database has never mirrored before or if hard drives
        # have been cycled use the recovery date
        time_stamp = settings['RECOVER_DATA_SINCE_DATE']
        # Times in the past in flux notation
        flux_times = ["-1m", "-1h", "-6h", "-12h", "-1d", "-7d", "-14d"]
        got_data = False
        for flux_time in flux_times:
            responseCSVIterator = localClientQuery.query_csv(f'from(bucket:"{bucket_name}") |> range(start: {flux_time}) |> sort(columns: ["_time"]) |> last()')

            for resp in responseCSVIterator: # Only enters loop if the iterator has data

                if got_data:
                    time_stamp = resp[time_idx]
                    break

                for i, entry in enumerate(resp):
                    if entry == "_time":
                        time_idx = i
                        got_data = True
                        break

            if got_data:
                break

        ## Request data time stamped after the most recent local data for this bucket ##
        print("Querying from: ", time_stamp)
        dataCSVIterator = remoteClientQuery.query_csv(f'from(bucket:"{bucket_name}") |> range(start: {time_stamp})')

        ## #Push each data point to the local client ###

        # Parse the query csv rows into writeable point objects
        need_headers = True
        custom_tags = []
        points=[]
        for resp in dataCSVIterator:
            # TODO: use these records to improve mirroring, skip for now
            if "#datatype" in resp or "#group" in resp or "#default" in resp:
                continue
            if not need_headers and "result" in resp:
                continue
            if need_headers and ('_value' in resp):
                need_headers = False
                custom_tags = {} # key: index
                for i, header in enumerate(resp):
                    if header == '':
                        empty_idx = i
                    elif header == 'result':
                        result_idx = i
                    elif header == 'table':
                        table_idx = i
                    elif header == '_start':
                        start_idx = i
                    elif header == '_stop':
                        stop_idx = i
                    elif header == '_time':
                        time_idx = i
                    elif header == '_value':
                        value_idx = i
                    elif header == '_field':
                        field_idx = i
                    elif header == '_measurement':
                        measurement_idx = i
                    else:
                        custom_tags[header] = i
            elif not need_headers:
                # Check data type of resp[value_idx]
                try:
                    val = float(resp[value_idx])
                except ValueError:
                    val = resp[value_idx]
                point_to_mirror = Point(resp[measurement_idx]).field(resp[field_idx],
                                                                  val).time(resp[time_idx])
                for tag_name, tag_idx in zip(custom_tags.keys(), custom_tags.values()):
                    point_to_mirror.tag(tag_name, resp[tag_idx])

                points += [point_to_mirror]

        ### Actually mirror the point to local ###
        print("Mirroring ", len(points), " data points")
        localClientWrite.write(bucket_name, settings["LOCAL_ORG"], record=points)
        logger(localClientWrite, settings, "DEBUG",
               f"Finished mirroring {len(points)} data points in the bucket: {bucket_name}")

def wait(settings, local_client_write):
    ### Wait the requested timeout period ###
    print("Waiting for ", settings['REFRESH_RATE'], " before trying to mirror.")
    logger(local_client_write, settings, "DEBUG",
           f"Waiting for {settings['REFRESH_RATE']} before trying to mirror.")
    refresh_rate = datetime.strptime(settings['REFRESH_RATE'], "%H:%M:%S")
    time_to_check_remote_db = datetime.now() + timedelta(hours=refresh_rate.hour,
                                                     minutes=refresh_rate.minute,
                                                     seconds=refresh_rate.second)
    while True:
        if time_to_check_remote_db <= datetime.now():
            break
        time.sleep(0.2)

if __name__ == "__main__":

    # Catch the use case where the isRunning.pickle file has the wrong values
    # do to a forced exit of this program (other than ctrl-c)
    if len(sys.argv) >= 2:
        arg_in = sys.argv[1]
    else:
        arg_in = None

    main(arg_in)
