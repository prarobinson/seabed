import sys
import psycopg2
#from worms_suds import *
from matplotlib import pyplot as plt
from argparse import ArgumentParser
import csv
import statistics as stat
import numpy as np
from datetime import datetime, timedelta
from collections import OrderedDict

def get_taxa(annot):
   with open('Species_Table.csv') as csv_file:
      csv_reader = csv.reader(csv_file, delimiter=',')
      for row in csv_reader:
         is_in = [i.lower() for i in row if i.lower()==annot.lower()]
         if len(is_in) != 0:
            taxa = [i.lower() for i in row]
            return taxa
         else:
            taxa = ["not listed on Species lookup table", annot]
 
   return taxa


def hist(data, label):
   ### takes a list of floats as 1st input, name of the data (str) as the 2nd
   print('\nGenerating histogram...\n')
   n, bins, patches = plt.hist(x=data, bins='auto', color='#0504aa', alpha=0.7, rwidth=0.85)
   plt.grid(axis='y', alpha=0.75)
   plt.xlabel('Value')
   plt.ylabel('Frequency')
   plt.title(label)
   plt.text(23, 45, r'$\mu=15, b=3$')
   maxfreq = n.max()
   # Set a clean upper y-axis limit.
   plt.ylim(ymax=np.ceil(maxfreq / 10) * 10 if maxfreq % 10 else maxfreq + 10)
   plt.show()




def main(dbname, user):
   conn = psycopg2.connect("dbname=%s user=%s" % (dbname, user))
   try:
      cursor = conn.cursor()
      
      prompt = "Select from the following options (or q to quit):\n\n"
      prompt += "  Whole database operations:\n"
      prompt += "\t 1) Show all cruise information\n"
      prompt += "\t 2) Show all dive information\n"
      prompt += "\t 3) Show primary species structure (fish_types.py for 12RB)\n"
      prompt += "\t 4) Show all organism types\n"
      prompt += "\t 5) Show all organism subtypes\n"
      prompt += "\t 6) Dump database as .csv files\n"
      prompt += "\n  Cruise-level queries:\n"
      prompt += "\t 7) Logistical information and notes for dives by cruise\n"
      prompt += "\n  Queries by dive:\n"
      prompt += "\t 8) Number of each organism subtype by dive\n"
      prompt += "\t 9) Number of fishcount files by dive\n"
      prompt += "\t10) Number of images by dive\n"
      prompt += "\t11) Images with with more than N organism(s) identified\n"
      prompt += "\t12) Produce organism histograms by dive\n"
      prompt += "\t13) Dive area surveyed by substrate\n"
      prompt += "\t14) Plot CTD data\n"
      prompt += "\t15) Export merged fct and ctd to .csv\n"
      prompt += "--> "
      
      while True:
         response = input(prompt)
         response = response.strip()
         
         if len(response) == 0 or "Q" == response or "q" == response:
            break

         choice = 0
         try:
            choice = int(response)
         except:
            pass

         
         if choice == 1:
            cols = ["cruise_id", "cruise_name", "ship_name"]
            cursor.execute("SELECT %s FROM cruise ORDER BY cruise_id" % ",".join(cols))
            
            for row in cursor:
               results = []
               for i, col in enumerate(cols):
                  results.append("%s: %s" % (col, row[i]))
               print(", ".join(results))

  
         elif choice == 2:
            cols = ["cruise.cruise_id", "directory", "location", "starttime", "endtime", "origin_lat", "origin_lon"]
            cursor.execute("SELECT %s FROM cruise, dive WHERE dive.cruise_id = cruise.id ORDER BY starttime" % ",".join(cols))
            
            for row in cursor:
               results = []
               for i, col in enumerate(cols):
                  results.append("%s: %s" % (col, row[i]))
               print(", ".join(results))
         

         elif choice == 3:
            ### TODO: generate python structure for 12RB
            print("Not implemented yet")



         elif choice == 4:
            cursor.execute("SELECT DISTINCT org_type FROM fct WHERE org_type <> '' ORDER BY org_type")
            results = []
            for row in cursor:
               results.append(row[0])
            print(", ".join(results))
            


         elif choice == 5:
            cursor.execute("SELECT DISTINCT org_subtype FROM fct WHERE org_subtype <> '' ORDER BY org_subtype")
            results = []
            for row in cursor:
               results.append(row[0])
            print(", ".join(results))


         elif choice == 6:
            response = input("\nPlease enter an output directory with write permissions: ")
            output_dir = (response.strip(),)
            SQL="CREATE OR REPLACE FUNCTION db_to_csv(path TEXT) RETURNS void AS $$ declare tables RECORD;   statement TEXT; begin FOR tables IN SELECT (table_schema || '.' || table_name) AS schema_table FROM information_schema.tables t INNER JOIN information_schema.schemata s ON s.schema_name = t.table_schema WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema') AND t.table_type NOT IN ('VIEW') ORDER BY schema_table LOOP statement := 'COPY ' || tables.schema_table || ' TO ''' || path || '/' || tables.schema_table || '.csv' ||''' DELIMITER '';'' CSV HEADER'; EXECUTE statement; END LOOP; return; end; $$ LANGUAGE plpgsql; SELECT db_to_csv(%s);"
            cursor.execute(SQL, output_dir)



         elif choice == 7:
            print('This function is not implemented yet...')
            continue
            SQL="select cruise.id, cruise.cruise_name from cruise;"
            cursor.execute(SQL)
            #cursor.fetchall()
            for row in cursor:
               print(row)
            response = input("\nPlease enter a cruise_id from the above: ")
            cruise_id = (response.strip(),)
            #SQL="select dive. , from dive where dive.cruise_id= %s);"
            #cursor.execute(SQL, cruise_id)


    
         elif choice == 8:
            response = input("\nEnter the dive directory (e.g., d20150917_1): ")
            dive = (response.strip(),)
            SQL='SELECT DISTINCT fct.org_subtype as Subtype FROM fct WHERE fct.dive_id=(select id from dive where directory = %s) order by Subtype;'
            cursor.execute(SQL, dive)
            results = []
            for row in cursor:
               #print(row)
               results.append(row[0])

            for result in results:
               SQL='select count(fct.org_subtype) from fct where fct.dive_id=(select id from dive where directory = %s) and  fct.org_subtype ILIKE %s;'
               cursor.execute(SQL, (dive, result))
               qreturn = cursor.fetchall()
               print(result, [x[0] for x in qreturn])
 

        
         elif choice == 9:
            ### TODO: report empty fct files (they have to get uploaded first, obviously, but seabed.py doesn't do this currently
            response = input("\nEnter the dive directory (e.g., d20150917_1): ")
            dive = (response.strip(),)
            SQL = "SELECT COUNT(distinct fct.originating_fct) FROM fct WHERE fct.dive_id=(select id from dive where directory = %s);"
            cursor.execute(SQL, dive)
            for row in cursor:
               print(row[0])

            


               
         elif choice == 10:
            response = input("\nPlease enter a dive directory (e.g., d20150917_1): ")
            dive_dir = (response.strip(),)
            SQL="select count(distinct fct.filename) from fct where fct.dive_id=(select id from dive where directory = %s);"
            cursor.execute(SQL, dive_dir)
            qreturn = cursor.fetchall()
            for row in qreturn:
               print(row[0])

         
         elif choice == 11:
            response = input("\nEnter minimum number of organisms: ")
            count = 1
            try:
               count = int(response.strip())
            except:
               continue
            
            cursor.execute("SELECT filename FROM fct GROUP BY filename HAVING COUNT(*) > %s", (count, ))
            results = []
            for row in cursor:
               results.append(row[0])
            print(", ".join(results))

         
         elif choice == 12:
            response = input("\nPlease enter a dive directory (e.g., d20150917_1): ")
            dive_dir = (response.strip(),)
            SQL="select fct.org_type, fct.org_subtype from fct where fct.dive_id=(select dive.id from dive where dive.directory = %s);"
            cursor.execute(SQL, dive_dir)
            org_type_list = []
            org_type_dict = {}
            print("\n Retrieving results...\n")
            for row in cursor:
               org_subtype = row[1]
               org_type = row[0]
               if org_subtype:
                  taxa = get_taxa(org_subtype)
               elif org_type:
                  taxa = get_taxa(org_type)
               else:
                  taxa = ["type and subtype not listed in db table"]

               for taxon in taxa:
                  if taxon in org_type_dict.keys():
                     org_type_dict[taxon] += 1
                  else:
                     org_type_dict[taxon] = 1

               if taxa not in org_type_list:
                  org_type_list.append(taxa)

            ### give QA diagnostsics
            if org_type_list[0] == ['type and subtype not listed in db table']:
               org_type_list.remove(org_type_list[0])

            print('\nUnique rows found on lookup table: (', sum([i[0]!='not listed on Species lookup table' for i in org_type_list]), ')\n')
            for idx in range(0, len(org_type_list)):
               if org_type_list[idx][0] != 'not listed on Species lookup table':
                  print(",".join([i for i in org_type_list[idx]]))

            print('\n\nNumber of instances of each taxon:\n') 
            org_type_dict_s = sorted(org_type_dict.items(), key=lambda x: x[1], reverse=True)
            ### first 2 entries are '' and 'not listed on Species lookup table', so let's drop those
            del org_type_dict_s[0:1]
            for org in org_type_dict_s:
               print(org[0], org[1])

            print('\n\nOrg_type or org_subtype not found on lookup table (check spelling): (', len(org_type_list) - sum([i[0]!='not listed on Species lookup table' for i in org_type_list]), ')\n')
            for idx in range(0, len(org_type_list)):
               if org_type_list[idx][0] == 'not listed on Species lookup table':
                  print(org_type_list[idx][1])

           ### option to update the DB with names from the Species lookup table
           #print('Would you like to change a field in the database? [y/N]')

         
         elif choice == 13:
            response = input("\nPlease enter a dive directory (e.g., d20150917_1): ")
            dive_dir = (response.strip(),)
            SQL="SELECT DISTINCT ON (f.filename) f.filename, f.img_area, f.dive_id, f.substrate, d.id, d.directory FROM fct f JOIN dive d on d.id = f.dive_id WHERE d.directory = %s ORDER BY f.filename, d.id;"
            cursor.execute(SQL, dive_dir)
            qreturn = cursor.fetchall()
            tot_area = sum([x[1] for x in qreturn])
            print("\nTotal area surveyed: " + "{0:.2f}".format(tot_area) + " m^2")
            
            print("   Substrate type:")
            subs = [x[3] for x in qreturn]
            list_set = set(subs)
            unique_subs = (list(list_set))
            area_by_sub = [0]*len(unique_subs)
            types_areas = []
            for i in range(0, len(unique_subs)):
               for row in qreturn:
                  if row[3] == unique_subs[i]:
                      area_by_sub[i] = area_by_sub[i] + row[1]
               outstr = '%17s  %9s  %1s' % (unique_subs[i], "{0:.2f}".format(area_by_sub[i]), "m^2")
               print(outstr)
               types_areas.append([ unique_subs[i], area_by_sub[i] ])
            
            ### return any fct files with non-2-letter substrate codes            
            SQL="select fct.originating_fct, fct.substrate from fct where substrate not like '__' and fct.dive_id=(select id from dive where directory = %s);"
            cursor.execute(SQL, dive_dir)
            qreturn = cursor.fetchall()
            print("\n  .fct files without 2-letter substrate code:")
            for row in qreturn:
               print(row[0])

         
         elif choice == 14:
            response = input("\nPlease enter a dive directory (e.g., d20150917_1): ")
            dive_dir = (response.strip(),)
            ### This gets the CTD data:
            SQL="select ctd.rovtime, ctd.dive_id, ctd.sal, ctd.temp, ctd.pres, ctd.sos from ctd where ctd.dive_id=(select id from dive where directory = %s);"
            cursor.execute(SQL, dive_dir)
            CTDqreturn = cursor.fetchall()

            ctd_times = [x[0] for x in CTDqreturn]
            ctd_sals = [x[2] for x in CTDqreturn]
            ctd_temps = [x[3] for x in CTDqreturn]
            ctd_depths = [x[4] for x in CTDqreturn]
            ctd_soss = [x[5] for x in CTDqreturn]
            
            ### This gets the OPTODE data:
            SQL="select optode.rovtime, optode.psat, optode.conc from optode where optode.dive_id=(select id from dive where directory = %s);"
            cursor.execute(SQL, dive_dir)
            OPTqreturn = cursor.fetchall()

            ### Now, we need to associate psat and conc to a depth, via a close rovtime  
            opt_times = [x[0] for x in OPTqreturn]
            opt_psats = [x[1] for x in OPTqreturn]
            opt_concs = [x[2] for x in OPTqreturn]
 
            ### for each timestamp in the optode table, go through the ctd times and select the time just prior to the optode time
            ### TODO: this method should be sufficient, but could it be better? Might be good to report the mean and sd of differencees between these timestamps
            opt_depths = []
            for idx, ot in enumerate(opt_times):
               for jdx, ct in enumerate(ctd_times):
                  if ct > ot:
                     opt_depths.append(ctd_depths[jdx-1])
                     break


            print("\nThis dive goes from ", min(ctd_depths),"m to ", max(ctd_depths), "m")
            depth_window = input("\nPlease enter the range of depths you're interested in (e.g. 3.0, 100.0): ")
            depth_range = [float(depth_window.split(',')[0]), float(depth_window.split(',')[1])]

            ### We should truncate data from depths less than 'shallow'
            ctd_depths_logic = [val > min(depth_range) and val < max(depth_range) for val in ctd_depths]
            ctd_depths_trunc = [val for idx, val in enumerate(ctd_depths) if ctd_depths_logic[idx] == True]
            ctd_temps_trunc = [val for idx, val in enumerate(ctd_temps) if ctd_depths_logic[idx] == True]
            ctd_sals_trunc = [val for idx, val in enumerate(ctd_sals) if ctd_depths_logic[idx] == True]

            opt_depths_logic = [val > min(depth_range) and val < max(depth_range) for val in opt_depths]
            opt_depths_trunc = [val for idx, val in enumerate(opt_depths) if opt_depths_logic[idx] == True]
            opt_psats_trunc = [val for idx, val in enumerate(opt_psats) if opt_depths_logic[idx] == True]
            opt_concs_trunc = [val for idx, val in enumerate(opt_concs) if opt_depths_logic[idx] == True]
            
            ### output some QA
            out_temps = []
            avg_temp = stat.mean(ctd_temps_trunc)
            sd2_temp = 2 * stat.stdev(ctd_temps_trunc)
            for tmp in ctd_temps_trunc:
               if tmp > avg_temp + sd2_temp:
                  out_temps.append(tmp)
               elif tmp < avg_temp - sd2_temp:
                  out_temps.append(tmp)
            if len(out_temps) > 0:
               print("\nTemps recorded more than 2 SD from the mean: \n mean: ", avg_temp, "\n Anomalies: ", out_temps)
               hist(ctd_temps_trunc, 'Temperatures')        

            out_sals = []
            avg_sal = stat.mean(ctd_sals_trunc)
            sd2_sal = 2 * stat.stdev(ctd_sals_trunc)
            for sal in ctd_sals_trunc:
               if sal > avg_sal + sd2_sal:
                  out_sals.append(sal)
               elif sal < avg_sal - sd2_sal:
                  out_sals.append(sal)
 
            if len(out_sals) > 0:           
               print("\nSalinities recorded more than 2 SD from the mean: \n mean: ", avg_sal, "\n Anomalies: ", out_sals)
               hist(ctd_sals_trunc,'Salinities')        

            fig, ax1 = plt.subplots()
            
            color = 'tab:green'
            ax1.set_ylabel('Depth (m)')
            color = 'blue'
            ax1.set_xlabel('Temp (C)', color=color)
            ax1.plot(ctd_temps_trunc, ctd_depths_trunc, color=color, marker='.', linestyle='None')
            ax1.tick_params(axis='x', labelcolor=color)
            
            ax2 = ax1.twiny()
            color = 'green'
            ax2.set_xlabel('Salinity (psu)', color=color)
            ax2.plot(ctd_sals_trunc, ctd_depths_trunc, color=color, marker='.', linestyle='None')
            ax2.tick_params(axis='x', labelcolor=color)
            
            ### Gotta add O2 from 'OPTODE'. This is 'psat'
            ax3 = ax1.twiny()
            color = 'red'
            ax3.set_xlabel('O2 (% saturation)', color=color)
            ax3.plot(opt_psats_trunc, opt_depths_trunc, color=color, marker='.', linestyle='None')
            ax3.tick_params(axis='x', labelcolor=color, pad=30)
            
            plt.gca().invert_yaxis()
            plt.tight_layout()
            plt.show()

            


         elif choice == 14:
            response = input("\nPlease enter an output directory with write permissions: ")
            output_dir = (response.strip(),)
            SQL="CREATE OR REPLACE FUNCTION db_to_csv(path TEXT) RETURNS void AS $$ declare tables RECORD;   statement TEXT; begin FOR tables IN SELECT (table_schema || '.' || table_name) AS schema_table FROM information_schema.tables t INNER JOIN information_schema.schemata s ON s.schema_name = t.table_schema WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema') AND t.table_type NOT IN ('VIEW') ORDER BY schema_table LOOP statement := 'COPY ' || tables.schema_table || ' TO ''' || path || '/' || tables.schema_table || '.csv' ||''' DELIMITER '';'' CSV HEADER'; EXECUTE statement; END LOOP; return; end; $$ LANGUAGE plpgsql; SELECT db_to_csv(%s);"
            cursor.execute(SQL, output_dir)



         elif choice == 15:
            print("Not implemented yet...")
            continue
            response = input("\nPlease enter a dive directory (e.g., d20150917_1): ")
            dive_dir = (response.strip(),)
            response = input("\nPlease enter an output directory with write permissions: ")
            output_dir = (response.strip(),)
            SQL=""
            cursor.execute(SQL, dive_dir)


      
   finally:
      conn.close()
   
   return 0
   

if __name__ == "__main__":
   parser = ArgumentParser(description="Select queries to perform against SeaBED database.")
   parser.add_argument("-n", "--dbname", dest="dbname", default="seabed", help="Name of database")
   parser.add_argument("-u", "--user", dest="user", default="seabed", help="Name of database user")
   ### Example: python queries.py -n seabed -u paulr
   args = parser.parse_args()
   sys.exit(main(args.dbname, args.user))
