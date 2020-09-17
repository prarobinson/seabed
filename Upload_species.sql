-- NOTE: The organism histogram query interrogates the Species_Table.csv directly, but we may wish to have this as a table instead. This table IS used to create fish_types.py for 12RB
CREATE TABLE species_list(
  ID SERIAL,
  ObjectID INTEGER,
  AphiaID INTEGER,
  RACEcode INTEGER,
  FRAMcode INTEGER,
  SWFSCcode VARCHAR,
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
  Morpho_type_descriptor VARCHAR,
  Also_called_historically_3 VARCHAR,
  Button_12RB VARCHAR,
  Group1 VARCHAR,
  Group2 VARCHAR,
  Activation_date DATE,
  Deactivation_date DATE,
  Notes VARCHAR,
  PRIMARY KEY (ID)
);

COPY  species_list(ObjectID,AphiaID,RACEcode,FRAMcode,SWFSCcode,Kingdom,Phylum,Subphylum,Class,Subclass,"Order",Family,Genus,Species,AUV_OTU,common_name,common_name_authority,AUV_nickname,morpho_type_descriptor,also_called_historically_3,Button_12RB,Group1,Group2,Activation_date,Deactivation_date,Notes)
 FROM '/home/paulr/WorkShtoof/NOAA/seabed/Species_Table.csv' DELIMITER ',' CSV HEADER;

