-- NOTE: The organism histogram query interrogates the Species_Table.csv directly, but we may wish to have this as a table instead. This table IS used to create fish_types.py for 12RB
CREATE TABLE seabed.species_list(
  ID SERIAL,
  Ref_ID INTEGER,
  Aphia_ID INTEGER,
  RACE_code INTEGER,
  FRAM_code INTEGER,
  HEP_code VARCHAR,
  Kingdom VARCHAR,
  Phylum VARCHAR,
  Subphylum VARCHAR,
  Class VARCHAR,
  Subclass VARCHAR,
  "Order" VARCHAR,
  Family VARCHAR,
  Genus VARCHAR,
  Species VARCHAR,
  AUV_OTU INTEGER,
  Common_name VARCHAR,
  Common_name_authority VARCHAR,
  AUV_nickname VARCHAR,
  Also_called_historically VARCHAR,
  12RB_category VARCHAR,
  12RB_category_abbr VARCHAR,
  12RB_layout VARCHAR,
  12RB_button VARCHAR,
  Grp_nonstandard_taxon VARCHAR,
  Date_activated DATE,
  Date_deactivated DATE,
  Notes VARCHAR,
  PRIMARY KEY (ID)
);

-- NOTE!!! Change the path to wherever your copy of the Species_Table.csv file lives
COPY  species_list(Ref_ID,Aphia_ID,RACE_code,FRAM_code,HEP_code,Kingdom,Phylum,Subphylum,Class,Subclass,"Order",Family,Genus,Species,AUV_OTU,common_name,common_name_authority,AUV_nickname,Also_called_historically,12RB_category,12RB_category_abbr,12RB_layout,12RB_button,Grp_nonstandard_taxon,Date_activated,Date_deactivated,Notes)
 FROM '/home/paulr/WorkShtoof/NOAA/seabed/AUV_Species_Table.csv' DELIMITER ',' CSV HEADER;

