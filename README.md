# SEABED
Project for archiving dive and annotated image NOAA data

## Example usage:

# Create seabed environment:
>conda env create -f environment.yml
>conda activate seabed 

# Upload data:

>python seabed.py seabed.sql [/path/to/data] -u [user] -n [database_name]

>python seabed.py seabed.sql /home/paulr/WorkShtoof/NOAA/FUL\_17\_01 -u paulr -n seabed


# Launch queries script:

>python queries.py -n seabed -u paulr
