# CryoscopeInfluxDBMirror

The contents of this directory facilitate safe mirroring of a remote InfluxDB database to a local database running on this machine.

## Prerequisites

Before starting this program several prerequisites need to be completed:

1. Have a remote influx database running that is accessible via an IP.
2. Have an admin token for the remote database.
3. The buckets in this remote database should be owned by the same influx organization.
4. Have a local influx database installed and running. If the local database is on this machine the `influxd` command can be used to start it.
5. Have an admin token for the local database.
6. The buckets in the local database should be owned by the same influx organization.
7. The local database should have an additional bucket named `Logging`. This bucket will be used to log mirror actions.
8. Buckets you want to mirror should have the same names on the local and remote databases.
9. Have python 3 installed with the additional packages `influxdb-client` and `pyyaml`.
10. Be sure any firewall allows the port for the database to be accessible.
11. It appears that higher versions of python don't work: use Python 3.9.13.

## Start Mirroring Data

1. Make sure the prerequisite steps have been completed.
2. Edit `settings.yaml` and fill it out with the proper information in YAML syntax. Summary of fields:

   | Key | Purpose | Notes |
   | --- | --- | --- |
   | `REMOTE_IP` | The IP of the remote database from which data will be pulled. | The IP should contain a port, as in `XXX.XXX.XXX.XXX:XXXX` |
   | `REMOTE_TOKEN` | The admin token of the remote database. | Should be the actual token, not the token's name in the influx GUI. |
   | `REMOTE_ORG` | The organization which owns all the buckets on the remote database. | |
   | `LOCAL_IP` | The IP of the local database which data will be mirrored to. | In most cases should be `http://localhost:8086`. |
   | `LOCAL_TOKEN` | The admin token of the local database. | Should be the actual token, not the token's name in the influx GUI. |
   | `LOCAL_ORG` | The organization which owns all the buckets on the local database. | |
   | `REFRESH_RATE` | A time delay that occurs between mirror attempts. Is triggered after a successful mirror and anytime a connection error with the remote database occurs. | The format is in hours, minutes, seconds: `HH:MM:SS` |
   | `RECOVER_DATA_SINCE_DATE` | If the local database has no datapoints at all, this date is used to retrieve all data on the remote database after this date. | The string format follows InfluxDB's standard RFC3339 timestamp, without subsecond precision. |
   | `BUCKETS` | A list of bucket names that will be mirrored. | The bucket names on the local and remote databases should be the same. |

   See the `settings.yaml` file for examples (without the tokens).

3. Start the mirror service from the command line (assuming you are in the `CryoscopeInfluxDBMirror` directory):

   **Mirroring will continue indefinitely:**
   1. Use correct environment, check which python and make sure it is python 3.9.13.
   2. `screen`
   3. `python main.py forceOn`
   4. Disconnect from screen with `Ctrl-A d`

   **Mirroring will stop when ssh session ends:**
   1. `python3 main.py` or `./main.py`

## Safely Stop the Mirroring

If mirroring started with `screen`:
- Find the screen with `screen -ls`
- Attach to screen with `screen -r`
- Kill the process with `Ctrl-C`

If mirroring started with `python3` or `./main.py`:
- Press `Ctrl-C`

## Recovering From Critical Errors and Unexpected Reboots

As a safety feature, the program uses a pickle file to keep track of whether it's been turned on or off. This prevents multiple instances of this program running, which could lead to unwanted mirroring behaviors. If the program doesn't exit gracefully with a keyboard interrupt signal (`Ctrl-C`), the pickle file will retain the state as being on, when in reality the program is not running. If the program is started while the pickle file thinks the state is on, it will print the following:

> This program is already running on this device, or exited due to an error. It can be forcibly started regardless of other instances with `python3 main.py forceOn`

To make sure this program is not running, use `ps -ef | grep python` and make sure there isn't a process associated with `main.py`. The full family of forced start commands are:

- Force start and mirror indefinitely: `screen`, then `./main.py forceOn`
- Force start and mirroring will stop when ssh session ends: `python3 main.py forceOn` or `./main.py forceOn`

## Developer Documentation and Notes

### Mirror Protocol

The steps for mirroring are designed to handle the remote database having an unstable connection. The step-by-step procedure is:

1. Ping the remote database. If a response is received, continue with the mirror attempt. If not, wait the specified cooldown (`REFRESH_RATE` in `settings.yaml`) before attempting to mirror again.
2. For each bucket name in `settings.yaml`:
   1. Query the local database to get the timestamp of the most up-to-date data present. If there is no data, use the recovery date in `settings.yaml`.
   2. Query the remote database for all data after the timestamp found in the previous step.
   3. Parse the datapoints into `Point` objects, which really just encode influx's line protocol.
   4. Write the new datapoints to the local database.

There is a slim possibility that the connection to the remote database is lost after a successful ping and before mirroring is completed. In this case, an error will be thrown by `urllib3`. This error is caught using a `try`/`except` python block, and is logged to the local database in the `Logging` bucket with measurement name `Logs`. This error doesn't result in a loss of data because when connection is regained the mirror protocol will use the timestamp of the most recent data present in the local database to query the remote.

A potential weakness of this protocol is that influx databases allow timestamps to be written with values other than the current time. This has two consequences:
- If data with a timestamp in the past is added to the remote database, it won't ever be mirrored to the local.
- If a datapoint with a timestamp significantly in the future is added to the remote database, it will be mirrored, but new datapoints with correct timestamps will not be mirrored in following mirror attempts until their timestamps eventually date them later than the initial faulty datapoint.

### Emergency Database Recovery

Throughout this documentation, the terms "local" and "remote" database have been used. While this program is designed to (and should be) run with a local influx database on the same machine, this isn't enforced. If one were to change the `LOCAL_IP` key in `settings.yaml` to an externally reachable IP pointing to an influx database, data would be mirrored to that IP. Currently, this is undesired because the "local" database is not pinged, and consequently there is no "safe" avoidance of sending HTTP POST requests to an unreachable IP. The error will be caught, but a new error will be created when the system attempts to log the initial error by writing to the "local" database's `Logging` bucket.

This unintended functionality has two useful applications:

1. It can be used to restore data between any two influx databases in an emergency. For example, if someone in Antarctica does a little `sudo rm -rf` on accident, switching the "local" and "remote" fields in `settings.yaml` will take all data from Caltech's server with timestamps after the recovery date and put it onto Antarctica's server. Although it will be prone to failing, it will not fail in a way that destroys/loses data. To be clear, if there are three databases, A, B, and C, any database can perform an emergency backup on any two databases (including itself).
2. Data can be mirrored from one remote database to another from a remote location if a ping system is implemented for both sides.