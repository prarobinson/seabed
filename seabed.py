#!/usr/bin/env python3

import os
import re
import sys
import time
import glob
import numpy
import shutil
import sqlite3
import psycopg2

import os.path
import scipy.io

from datetime import datetime
from argparse import ArgumentParser

from config import config



### TODO: do we need to do this anymore?
# TODO: do we need to use the parsed schema to insert the sysconfig data?

# make any necessary adjustments to the data before storing to database
#def adjust(nav_t):
#   # duplicate the goal_str values for GOAL, e.g., [1, 2, 3] -> [1, 1, 2, 2, 3, 3]
#   # the insert function conveniently makes it easy to do duplicates as we need
#   data = nav_t["GOAL"][0,0]
#   data["goal_str"] = numpy.insert(data["goal_str"], range(len(data["goal_str"])), data["goal_str"])


def main(conn, tablemap, filemap, debug): 
   ### we can immediately pop the cruise, dive, and fct tables as those need to be done manually
   tablemap.pop("cruise", None)
   tablemap.pop("dive", None)
   tablemap.pop("fct", None)

   ### put the sebas tables into a separate dict
   sebasmap = {}
   sebasmap["frames"] = tablemap.pop("frames", None)
   sebasmap["targets"] = tablemap.pop("targets", None)
   
   syscfgmap = {}
   ### note we shallow copy into a list as we have to remove while iterating
   for key in list(tablemap.keys()):
      ### TODO: handle dive camera configuration data
      # this is a bit ugly but no better way -- NOT POPPING 'camera' for now, though we may need to add it. 
      if key in ["deltaT", "blueview", "logger"]:
         syscfgmap[key] = tablemap.pop(key)
   
   ### create cursor
   cursor = conn.cursor()
   
   try: 
      ### loop over every dive in the filemap
      for root, entry in sorted(filemap.items()):
         print("Processing dive: %s" % os.path.basename(root))
         
         syscfgfile = entry["syscfg"]
         syscfgpath = os.path.join(root, syscfgfile)
      
         print("Processing syscfg file: %s" % syscfgfile)
      
         metadata = parse_syscfg(syscfgmap, syscfgpath)
         syscfg = metadata.pop("syscfg")[0]
      
         ### first get the id of the cruise or create a new entry
         cursor.execute("SELECT id FROM seabed.cruise WHERE vehicle_name = %s AND cruise_id = %s", (syscfg["vehicle_name"], syscfg["cruise_id"]))
         if cursor.rowcount < 1:
            cursor.execute(
               "INSERT INTO seabed.cruise (vehicle_name, vehicle_cfg, cruise_name, cruise_id, ship_name, chief_sci) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id", 
               (
                  syscfg["vehicle_name"], syscfg["vehicle_cfg"], syscfg["cruise_name"], 
                  syscfg["cruise_id"], syscfg["ship_name"], syscfg["chief_sci"]
               )
            )
            conn.commit()
      
         cruiseid = cursor.fetchone()[0]
         
      
         ### get the date of the dive from the filename (probably not necessary)
         filetime = datetime.strptime(syscfgfile.split(".")[0], "%Y%m%d_%H%M")
         
         ### try getting the main directory (as given by the logger) if it's available, otherwise use root directory
         directory = None
         try:
            directory = os.path.basename(metadata["logger"][0]["log_dir"])
         except:
            directory = os.path.basename(root)
      

         ctlfiles = entry['ctls']

         ### get the start and end times for the metadata table (recall that the dicts are reverse-ordered)
         ### TODO: start[end]time seems to be used to add this back into rovtime, from whence it was subtracted originally. Do we want unix time, or unix time - starttime? Or do we want diffrent things for different tables? For now, we'll just use unix time and calculate elapsed time in any queries.
         first_ctl = open(os.path.join(root, ctlfiles[len(ctlfiles)-1]), 'r')
         first_time = first_ctl.readline().split()[1]
         starttime = datetime.utcfromtimestamp(float(first_time))

         last_ctl = open(os.path.join(root, ctlfiles[0]), 'r', encoding="latin-1")
         for line in last_ctl:
            pass

         last_time = line.split()[1]
         endtime = datetime.utcfromtimestamp(float(last_time))

         ### insert the metadata and get the dive id
         try:
            cursor.execute(
               "INSERT INTO seabed.dive (cruise_id, directory, filename, filetime, starttime, endtime, ready, location, origin_lat, origin_lon, utm_zone, utm_x, utm_y, mag_variation) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", 
               (
                  cruiseid, directory, syscfgfile, filetime, starttime, endtime, False, syscfg.get("location"), 
                  syscfg.get("origin_lat"), syscfg.get("origin_lon"), syscfg.get("utm_zone"), syscfg.get("utm_x"), 
                  syscfg.get("utm_y"), syscfg.get("mag_variation")
               )
            )
         
         except psycopg2.IntegrityError:
            print("This dive appears to have been processed already (but you may want to verify!); checking for additional dives ...")
            print()
            continue
         
         finally:
            conn.commit()
      
         diveid = cursor.fetchone()[0]
     
         for field, value in syscfgmap.items():
            if field not in metadata:
               continue
            
            print("Processing syscfg field: %s" % field)
            insert_syscfg(cursor, diveid, metadata, field, value["cols"], value["types"])
            conn.commit()
      
         elapsed = 0.0
         start = time.time()

         for ctl in ctlfiles:
            ctlpath = os.path.join(root, ctl)
            f = open(ctlpath, 'r',encoding="latin-1")
            ### iterate over every line in CTL file, grep for 'EST', 'REF', and 'THR', and insert line into db
            print("Processing CTL: %s ..." % ctlpath)
            for line in f:
               if re.search('EST', line):
                  insert_line(conn, cursor, diveid, line, 'seabed.est', tablemap['seabed.est']['cols'], tablemap['seabed.est']['types'])
               if re.search('REF', line):
                  insert_line(conn, cursor, diveid, line, 'seabed.traj', tablemap['seabed.traj']['cols'], tablemap['seabed.traj']['types'])
               if re.search('THR', line):
                  insert_line(conn, cursor, diveid, line, 'seabed.thr', tablemap['seabed.thr']['cols'], tablemap['seabed.thr']['types'])

            secs = time.time() - start
            print("(%.1fs)" % secs)
            elapsed += secs

         rawfiles = entry['raws']
         for raw in rawfiles:
            rawpath = os.path.join(root, raw)
            f = open(rawpath, 'r', encoding="latin-1")
            ### iterate over every line in RAW file, grep for 'BATTERY', 'CAMERA', 'OCTANS', 'OPTODE', 'PAROSCI', 'RDI', 'THR_PORT', 'THR_STBD', and 'THR_VERT', and insert line into db
            print("Processing RAW: %s ..." % rawpath)
            for line in f:
               if re.search('BATTERY', line):
                  ### just get the first 8 values. Not the prettiest, but here's one way.
                  line_trunc = line.split()[0:10]
                  line_trunx = []
                  for i in line_trunc:
                     line_trunx.append(re.sub(i, i + "X", i))
                
                  line = re.sub('X', ' ', "".join(line_trunx))
                  insert_line(conn, cursor, diveid, line, 'seabed.battery', tablemap['seabed.battery']['cols'], tablemap['seabed.battery']['types'])

               if re.search('CAMERA', line):
                  insert_line(conn, cursor, diveid, line, 'seabed.camera', tablemap['seabed.camera']['cols'], tablemap['seabed.camera']['types'])

               if re.search('OCTANS', line):
                  insert_line(conn, cursor, diveid, line, 'seabed.octans', tablemap['seabed.octans']['cols'], tablemap['seabed.octans']['types'])
   
               ### NOTE!!: added "0" item to end of the line to accomodate missing calibrated psat
               if re.search('OPTODE', line):
                  insert_line(conn, cursor, diveid, line, 'seabed.optode', tablemap['seabed.optode']['cols'], tablemap['seabed.optode']['types'])

               if re.search('PAROSCI', line):
                  ### Need to copy msw into depth field
                  line_spli = line.split()
                  line_spli.append(line_spli[2])
                  line_splix = []
                  for i in line_spli:
                     line_splix.append(re.sub(i, i + "X", i))
                
                  line = re.sub('X', ' ', "".join(line_splix))
                  insert_line(conn, cursor, diveid, line, 'seabed.paro', tablemap['seabed.paro']['cols'], tablemap['seabed.paro']['types'])

               if re.search('RDI', line):
                  ### just get the first 6 values.
                  line_trunc = line.split()[0:7]
                  line_trunx = []
                  for i in line_trunc:
                     line_trunx.append(re.sub(i, i + "X", i))
                
                  line = re.sub('X', ' ', "".join(line_trunx))
                  insert_line(conn, cursor, diveid, line, 'seabed.rdi', tablemap['seabed.rdi']['cols'], tablemap['seabed.rdi']['types'])

               if re.search('THR_PORT', line):
                  insert_line(conn, cursor, diveid, line, 'seabed.thr_port', tablemap['seabed.thr_port']['cols'], tablemap['seabed.thr_port']['types'])

               if re.search('THR_STBD', line):
                  insert_line(conn, cursor, diveid, line, 'seabed.thr_stbd', tablemap['seabed.thr_stbd']['cols'], tablemap['seabed.thr_stbd']['types'])

               if re.search('THR_VERT', line):
                  insert_line(conn, cursor, diveid, line, 'seabed.thr_vert', tablemap['seabed.thr_vert']['cols'], tablemap['seabed.thr_vert']['types'])

               if re.search('SEABIRD', line):
                  insert_line(conn, cursor, diveid, line, 'seabed.ctd', tablemap['seabed.ctd']['cols'], tablemap['seabed.ctd']['types'])

            secs = time.time() - start
            print("(%.1fs)" % secs)
            elapsed += secs

         
         ### update our dive table now that the data have been inserted
         cursor.execute("UPDATE seabed.dive SET ready = %s WHERE id = %s", (True, diveid))
         conn.commit()


         ### handle FCT files
         ### TODO: identify and automatically skip duplicate lines? For now they simply fail and we press on...
         fctfiles = entry.get("fcts", None)
         if fctfiles and len(fctfiles) > 0:
            print("Processing %d fct file(s)" % len(fctfiles),)
            start = time.time()
            
            for filename in fctfiles:
               this_file = os.path.join(root, filename)
               mod_date_epoch = os.path.getmtime(this_file)
               mod_date = time.ctime(mod_date_epoch)
               with open(os.path.join(root, filename), "r", encoding="latin-1") as fctfile:
                  ### TODO: if there are no non-empty lines nothing is put in the DB;do we want to capture that this empty fct file exists, so it can be queried later?
                  for line in fctfile:
                     line = line.strip()
                     
                     if len(line) == 0:
                        continue
                     
                     parts = line.split(",")
                     if len(parts) != 18:
                        continue
                     
                     ### sometimes the parts are empty which fails when cast to a number so fix those
                     for i, part in enumerate(parts):
                        if (i in [0, 1, 2, 6, 7, 8, 13, 14, 15, 16]) and len(part) == 0:
                           parts[i] = "nan"
                     
                     try:
                     ### getting a lot of these: "not all arguments converted during string formatting" after adding filename...
                        SQL = 'INSERT INTO seabed.fct (dive_id, latitude, longitude, depth, originating_fct, filename, time, img_area, img_width, img_height, substrate, org_type, org_subtype, index, org_x, org_y, org_length, org_area, mod_date, comment) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        cursor.execute(SQL, (diveid, float(parts[0]), float(parts[1]), float(parts[2]), str(fctfile.name), parts[3], datetime.strptime("%s %s" % (parts[4], parts[5]), "%Y/%m/%d %H:%M:%S.%f"), float(parts[6]), int(parts[7]), int(parts[8]), parts[9], parts[10], parts[11], parts[12], float(parts[13]), float(parts[14]), float(parts[15]), float(parts[16]), mod_date, parts[17]))
                     except Exception as e:
                        print("\nProblem entering FCT file", filename, "because:", e,)
                        conn.rollback()
                     else:
                        conn.commit()
                        continue

            secs = time.time() - start
            print("(%.1fs)" % secs)
            elapsed += secs
         

         #### handle SEBAS file
         sebasfile = entry.get("sebas", None)
         if sebasfile:
            print("Processing sebastes file: %s" % sebasfile,)
            start = time.time()
            
            insert_sebas(cursor, diveid, os.path.join(root, sebasfile), sebasmap)
            
            secs = time.time() - start
            print("(%.1fs)" % secs)
            elapsed += secs
         
         ####
      
         print("Total time: %.1fs" % elapsed)
         print()
   
   finally:
      cursor.close()
   
   return 0
   
   
def insert_syscfg(cursor, diveid, metadata, field, cols, types):
   stmt = "INSERT INTO %s values (%s)" % (field, ",".join(["%s"] * (len(cols) + 1)))
   
   ### note that row is a dict
   for row in metadata[field]:
      inserts = [diveid]
      
      for col in cols:
         if col in row:
            inserts.append(row[col])
      
      ### make sure we have all the fields
      ### TODO: do we always expect to have all the fields?
      if len(inserts) == (len(cols) + 1):
         cursor.execute(stmt, inserts)
   
   
def insert_line(conn, cursor, diveid, line, table, cols, types):
   ### add a space after any ':' so that these terms can be dumped and only their corresponding values retained
   line = re.sub(':', ': ', line)
   line_list = line.split()

   ### pop first element (now we can iterate starting at [0], below
   line_list.pop(0)
   ### get rid of elements containing ':'
   line_list = [ x for x in line_list if ":" not in x ]
   ### we need one extra column to account for the column id
   stmt = 'INSERT INTO {0} VALUES ({1})'.format(table, ", ".join(['%s'] * (len(cols) + 1)))
   ### list of values to insert starting with diveid
   inserts = [diveid]

   for i in range(0, len(line_list)):
      if line_list[i] == '-12345.000':
         value = None
      else: 
         dtype = types[i]
         if dtype == "float":
            value = float(line_list[i])
         elif dtype == "int":
            value = int(line_list[i])
         elif dtype == "str":
            value = str(line_list[i])
         elif dtype == "datetime":
            value = datetime.utcfromtimestamp(float(line_list[i]))
      
      inserts.append(value)

   ### MAKE SURE ALL EXPECTED DATA ARE PRESENT!! DATA LINE IS CURRENTLY OMITTED IF INCOMPLETE!!!
   if len(inserts)-1 != len(cols):
      print('Data appear to be incomplete:', table, cols, inserts)
      return

   try:
      cursor.execute(stmt, inserts)
      
   ### if there's an integrity error we need to rollback
   except psycopg2.IntegrityError:
      conn.rollback()
   
   ### since we might roll back we need to commit every time
   else:
      conn.commit()



def insert_sebas(cursor, diveid, sqlfile, tablemap):
   frames = []
   targets = []
   
   ### first get the data out of the sqlite file
   with sqlite3.connect(sqlfile) as conn:
      c = conn.cursor()
      ### build a list of frame and target rows and prepend the dive id
      for row in c.execute("SELECT * FROM seabed.frames"):
         frames.append((diveid,) + row)
      
      for row in c.execute("SELECT * FROM seabed.targets"):
         targets.append((diveid,) + row)
         
   ### then, if we have data, put it into the master database (assuming the table schemas match) 
   if len(frames) > 0:
      ### note we need to add one to the length because the foreign key is omitted in the dict
      cursor.executemany("INSERT INTO seabed.frames VALUES (%s)" % ",".join(["%s"] * ( len(tablemap["frames"]["cols"]) + 1 )), frames)
      conn.commit()
      
   if len(targets) > 0:
      ### note we need to add one to the length because the foreign key is omitted in the dict
      cursor.executemany("INSERT INTO seabed.targets VALUES (%s)" % ",".join(["%s"] * ( len(tablemap["targets"]["cols"]) + 1 )), targets)
      conn.commit()


def parse_syscfg(tablemap, filename):
   ### TODO: some CAMERA data are in the syscfg file, so we don't really want to pop it out of tablemap (see above)
   ### make a shallow copy so we can add to it
   tablemap = dict(tablemap)
   
   ### the syscfg section is part cruise and part dive so not defined in the database cleanly
   tablemap["syscfg"] = {
      ### TOPSIDE had longitude while FISHERIES had origin_lon
      "cols" : ["vehicle_name", "vehicle_cfg", "cruise_name", "cruise_id", "ship_name", "chief_sci", "location", "origin_lat", "origin_lon", "longitude", "utm_zone", "utm_x", "utm_y", "mag_variation"],
      "types" : [] # don't worry about types for now
   }
      
   datas = {}
   
   with open(filename, 'r') as syscfg:
      section = None
      data = {}
      
      for line in syscfg:
         line = line.strip()
         if line.startswith("#"):
            continue
            
         if section == None:
            if line.startswith("BEGIN:"):
               section = line.split()[1]
               if section not in tablemap:
                  section = None
         
         else:
            if line.startswith("END:"):
               if section in datas:
                  datas[section].append(data)
               else:
                  datas[section] = [ data ]
               
               data = {}
               section = None
               
            else:
               line = line.partition("#")[0]
               parts = line.split(":")
               if len(parts) > 1:
                  key = parts[0].strip()
                  if key in tablemap[section]["cols"]:
                     data[key] = parts[1].strip()
   
   return datas


def parse_schema(filename):
   ### this maps the postgres data types to python types
   typemap = {
      "DOUBLE" : "float",
      "REAL" : "float",
      "NUMERIC" : "float",
      "INTEGER" : "int",
      "VARCHAR" : "str",
      "TEXT" : "str",
      "SERIAL" : "str",
      "TIMESTAMP" : "datetime",
      "BOOLEAN" : "bool"
   }
   
   tablemap = {}
   
   ### build the map by parsing the file
   with open(filename, "r") as sql:
      table = None
      for line in sql:
         line = line.strip()
         if len(line) == 0:
            continue
      
         if table:
            if line.startswith("UNIQUE") or line.startswith("PRIMARY"):
               continue
               
            if line.startswith(");"):
               table = None
         
            elif line.startswith("--"):
               continue
            
            elif "REFERENCES" in line:
               continue
         
            else:
               parts = re.split("[\s+\(,]", line)
               tablemap[table]["cols"].append(parts[0])
               tablemap[table]["types"].append(typemap[parts[1]])
         
         else:
            if line.startswith("CREATE TABLE"):
               table = line.split()[2]
            
               tablemap[table] = {
                  "cols" : [],
                  "types" : []
               }
   
   return tablemap


### returns a map of the file pairs as:
###  root -> {syscfg: filename, raws: [filenames], ctls: [filenames], fcts: [filenames]}
def find_files(path):
   result = {}
   for root, dirs, files in os.walk(path):
      syscfgfile = None
      ctls = []
      raws = []
      sebasfile = None
      fctfiles = []
      ### Note, we're reverse sorting so we grab the newest file as sometimes
      ### there are multiple files for a single dive because of restarts.
      ### However, this also reverses the order in which CTL and RAW files are processed; do we care?
      for file in sorted(files, reverse=True):
         if (syscfgfile is None) and file.endswith(".FISHERIES.syscfg"):
            syscfgfile = file
         elif file.endswith("CTL.auv"):
            ctls.append(file)
         elif file.endswith("RAW.auv"):
            raws.append(file)   
         elif (sebasfile is None) and file.lower().endswith("sebastes.sql3"):
            sebasfile = file
         
      if syscfgfile:
         result[root] = { 
            "syscfg" : syscfgfile, 
            "ctls" : ctls,
            "raws" : raws,
            "sebas" : sebasfile
         }
   
   ### Now that we've found our syscfg, ctl, raw, and sebas files, go look in a specific place for the fcts
   for root in result:
      fcts = []
      
      ### use the all fish files if we have one
      allfish = False
      for allfct in glob.glob(os.path.join(root, "images/12RB", "*AllFish.fct")):
         allfish = True
         fcts.append(os.path.join("images/12RB", os.path.basename(allfct)))

      ### otherwise grab all of the fish count files
      if not allfish:
         for fct in glob.glob(os.path.join(root, "images/12RB", "*.fct")):
            fcts.append(os.path.join("images/12RB", os.path.basename(fct)))
   
      if len(fcts) > 0:
         result[root]["fcts"] = fcts
   
   return result

if __name__ == "__main__":
   parser = ArgumentParser(description="Process SeaBED files and insert into database.")
   parser.add_argument("schema", help="SQL schema file for database creation (note that database isn't necessarily [re-]created)")
   parser.add_argument("path", help="File system path to collection of data files")
   parser.add_argument("-d", "--debug", dest="debug", action="store_true", help="Use debug mode which reduces data processing")
   #parser.add_argument("-n", "--dbname", dest="dbname", default="seabed", help="Name of database")
   #parser.add_argument("-s", "--server", dest="host", default="nwcdbp24.nwfsc.noaa.gov", help="server name")
   #parser.add_argument("-p", "--port", dest="port", default="5455", help="port")
   #parser.add_argument("-w", "--pass", dest="password", default="", help="password")
   #parser.add_argument("-u", "--user", dest="user", default="seabed", help="Name of database user")
   parser.add_argument("-r", "--recreate", dest="drop", metavar="SCHEMA", help="If this option is provided the table will first be emptied and re-populated for the given dive; note that the argument should be the SQL schema file 'drop.sql'")
   parser.add_argument("-a", "--archive", dest="archive", metavar="PATH", help="If this option is provided the given data path will first be archived (copied) to the given archive path; note that destination path cannot exist")


   #### EXAMPLE (local):
   ####    python seabed.py seabed.sql /home/paulr/WorkShtoof/NOAA/FUL_17_01/d20170711_2 -r drop.sql  
   #### EXAMPLE with re-create:
   ####    python seabed.py seabed.sql /home/paulr/WorkShtoof/NOAA/FUL_17_01 -r drop.sql
   # parse argument and options responses 
   args = parser.parse_args()
   #print(args)
   ### This is the path to the data; see EXAMPLE above 
   path = args.path
   
   if args.archive is not None:
      print("Copying files from '%s' to '%s' ..." % (path, args.archive),)
      ### need this or the copying happens before printing
      sys.stdout.flush()
      
      shutil.copytree(path, args.archive)
      print("done\n")
      
      path = args.archive
   
   # first parse the database schema file as this is used as a template
   # for which fields to insert
   tablemap = parse_schema(args.schema)
   
   # find all of the cruise files
   filemap = find_files(path)
   
   # return code of program
   code = 0
   
   # setup the connection and pass to main
   conn = None
   params = config()

   print('Connecting to the PostgreSQL database...')
   conn = psycopg2.connect(**params)
   
   try:
      # if drop specified then delete from the database ONLY AT THE DIVE LEVEL (easy enough to iterate over dives in BASH)
      if args.drop:
         print("Emptying tables for dive ", os.path.basename(path))
         cursor = conn.cursor()
         ### the dive folder needs to be repeated for every replacement in the SQL statement -- including comments!
         dive_tuple = (os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),os.path.basename(path),)
         ### get code from drop.sql
         drop_code = open('drop.sql', 'r').read().splitlines()
         ### join it back up into astringsinceit was list-ified with split_lines to omit \n     
         SQL = ''.join(str(elem) for elem in drop_code)
         cursor.execute(SQL, dive_tuple)
         #cursor.execute(open(args.drop, "r").read())
         conn.commit()
         cursor.close()
         
      # and now call the main function with these maps
      code = main(conn, tablemap, filemap, args.debug)
      
   finally:
      conn.close()
   
   sys.exit(code)
