# SEABED
Project for archiving dive and annotated image NOAA data

## Example usage:

### To create local database for testing:
Have PostgreSQL installed (platform dependent)

log in (>psql)

>CREATE DATABASE seabed;

>\c seabed;

Copy all the code in seabed.sql into your terminal.

Now you should have an empty DB to populate using the steps below.


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

### Upload data (entire cruise or by dive):
>python seabed.py seabed.sql [/path/to/data] -u [user] -n [database_name]

>python seabed.py seabed.sql /home/paulr/WorkShtoof/NOAA/FUL\_17\_01 -u paulr -n seabed


### Launch queries script:
>python queries.py -n seabed -u paulr
