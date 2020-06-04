# seabed
Project for archiving dive and annotated image NOAA data

Example usage:

To upload data:

   python seabed.py seabed.sql [/path/to/data] -u [user] -n [database_name]

   python seabed.py seabed.sql /home/paulr/WorkShtoof/NOAA/FUL\_17\_01 -u paulr -n seabed


To launch queries script:

   python queries.py -n seabed -u paulr
