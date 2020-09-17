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

>CREATE DATABASE seabed;

>\c seabed;

Copy all the code in seabed.sql into your terminal.

Now you should have an empty DB to populate using the steps below.



### Upload data (entire cruise or by dive):
#### Local
NOTE: -s (host), -w (password), and -p (port) aren't used in this case

>python seabed.py seabed.sql [/path/to/data] -n seabed -u [user] -s "" -w "" -p ""

#### Remote server
-s (host) and -p (port) default to these values. You'll need to provide your pass word

>python seabed.py seabed.sql [/path/to/data] -n auv -u [user] -s nwcdbp24.nwfsc.noaa.gov -p 5455 -w [password]


### Launch queries script:
>python queries.py -n [auv/seabed] -u paulr
