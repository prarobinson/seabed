# SEABED
Project for archiving dive and annotated image NOAA data

## Example usage:

### Install Anaconda:
Install Anaconda 

Linux: https://docs.anaconda.com/anaconda/install/linux/

Mac: https://docs.anaconda.com/anaconda/install/mac-os/

Windows: https://docs.anaconda.com/anaconda/install/windows/


Curt's doc for Mac:

https://docs.google.com/document/d/1RLN141-h5eM0MVeWmS2QjfKAzpldNpSRSUe1_ag7P1E/edit


### Create the seabed environment:
>conda env create -f environment.yml

>conda activate seabed


### To create local database for testing:
Have PostgreSQL installed (this may be platform-dependent)

log in (>psql)

>CREATE DATABASE auv;

>\c auv;

>\CREATE SCHEMA seabed;

Copy all the code in seabed.sql into your terminal.

If there were no errors, you should have an empty DB to populate using the steps below.



### Upload data (entire cruise or by dive):
Connecting to the database is now handled using a .ini file, which simplifies the command line call considerably.

Make sure you're using the correct .ini file: databse.ini should point to the remote server, while database_local.ini is for your localhost. You will need to edit these files to contain your password!

>python seabed.py seabed.sql [/path/to/data] 

If an upload fails in the middle or you'd like to re-upload for any reason, simply add '-r drop.sql' to your call to empty all tables of data associated with this dive. Note: there is no DROP per se happening here, just DELETEs.


### Launch queries script:
>python queries.py

