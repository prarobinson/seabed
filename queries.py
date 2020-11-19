#!/usr/bin/env python3

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
from config import config
import math



### TODO: change this so it pulls from the DB table instead of this .csv file 
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



def cal_O2(O2_conc, temp, sal, pres):
   ### NOTE! Salinity setting is assumed to be 0!!! 
   sal_set = 0.0
   pres_comp = 0.032
   B0 = -0.00624097 
   B1 = -0.00693498
   B2 = -0.00690358
   B3 = -0.00429155
   C0 = -0.000000311680
   scaled_temp = np.log((298.15 - temp)/(273.15 + temp))
   sal_comp = math.exp((sal - sal_set)*(B0 + B1*scaled_temp + B2*scaled_temp**2 + B3*scaled_temp**3) + C0*(sal**2 - sal_set**2))
   pres_comp_factor = (((np.abs(pres))/1000) * pres_comp) + 1
   O2_adj = O2_conc * sal_comp * pres_comp_factor
   return O2_adj


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




def main():
   #conn = psycopg2.connect("dbname=%s user=%s server=%s port=%s password=%s" % (args.dbname, args.user, args.server,  args.port,args.password))
   conn = None

   try:
      params = config()

      print('Connecting to the PostgreSQL database...')
      conn = psycopg2.connect(**params)
		     
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
      prompt += "\t14) Plot CTD+Optode data\n"
      prompt += "\t15) Export merged fct and CTD+Optode to .csv\n"
      prompt += "\t16) Export all dive data to .csv\n"
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

         ### SHOW CRUISE INFO
         if choice == 1:
            cols = ["seabed.cruise.cruise_id", "seabed.cruise.cruise_name", "seabed.cruise.ship_name"]
            cursor.execute("SELECT %s FROM seabed.cruise ORDER BY seabed.cruise.cruise_id" % ",".join(cols))
            
            for row in cursor:
               results = []
               for i, col in enumerate(cols):
                  results.append("%s: %s" % (col, row[i]))
               print(", ".join(results))


         ### SHOW ALL DIVES
         elif choice == 2:
            cols = ["seabed.cruise.cruise_id", "seabed.dive.directory", "seabed.dive.location", "seabed.dive.starttime", "seabed.dive.endtime", "seabed.dive.origin_lat", "seabed.dive.origin_lon"]
            cursor.execute("SELECT %s FROM seabed.cruise, seabed.dive WHERE seabed.dive.cruise_id = seabed.cruise.id ORDER BY seabed.dive.starttime" % ",".join(cols))
            
            for row in cursor:
               results = []
               for i, col in enumerate(cols):
                  results.append("%s: %s" % (col, row[i]))
               print(", ".join(results))
         
 
         ### SHOW PRIMARY SPECIES STRUCTURE (fish_types.py)
         elif choice == 3:
            ### TODO: generate python structure for 12RB
            print('''
from collections import OrderedDict

# Labels for categories of species - tab alternatives in the left panel
ROCKFISH = 'Rockfish'
FLATFISH = 'Flatfish'
SKATES_SHARKS = 'Skates/Sharks'
ROUNDFISH = 'Roundfish'
INVERTEBRATES = 'Invertebrates'
CORALS = 'Corals'
SPONGES = 'Sponges'

# Abbreviations to use on annotation
fish_codes = {
    ROCKFISH: 'RO',
    FLATFISH: 'FL',
    SKATES_SHARKS: 'SK',
    ROUNDFISH: 'RN',
    INVERTEBRATES: 'IN',
    CORALS: 'CO',
    SPONGES: 'SP'
}

            '''
            )
            


         ### SHOW ALL ORGANISM TYPES
         elif choice == 4:
            cursor.execute("SELECT DISTINCT org_type FROM seabed.fct WHERE org_type <> '' ORDER BY org_type")
            results = []
            for row in cursor:
               results.append(row[0])
            print(", ".join(results))
            

         ### SHOW ALL ORGANISM SUBTYPES
         elif choice == 5:
            cursor.execute("SELECT DISTINCT org_subtype FROM seabed.fct WHERE org_subtype <> '' ORDER BY org_subtype")
            results = []
            for row in cursor:
               results.append(row[0])
            print(", ".join(results))


         ### DUMP DATABASE TO .CSV FILES
         ### TODO: This seems to work, but do we need to edit it to accommodate the "seabed" prefix?
         elif choice == 6:
            response = input("\nPlease enter an output directory with write permissions (full path name): ")
            output_dir = (response.strip(),)
            print("Exporting to ", output_dir[0])
            SQL="CREATE OR REPLACE FUNCTION db_to_csv(path TEXT) RETURNS void AS $$ declare tables RECORD;   statement TEXT; begin FOR tables IN SELECT (table_schema || '.' || table_name) AS schema_table FROM information_schema.tables t INNER JOIN information_schema.schemata s ON s.schema_name = t.table_schema WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema') AND t.table_type NOT IN ('VIEW') ORDER BY schema_table LOOP statement := 'COPY ' || tables.schema_table || ' TO ''' || path || '/' || tables.schema_table || '.csv' ||''' DELIMITER '';'' CSV HEADER'; EXECUTE statement; END LOOP; return; end; $$ LANGUAGE plpgsql; SELECT db_to_csv(%s);"
            cursor.execute(SQL, output_dir)


         ### LOGISTICAL AND OTHER INFO/COMMENTS FOR DIVES BY CRUISE
         elif choice == 7:
            print('This function is not implemented yet...')
            continue
            SQL="select cruise.id, cruise.cruise_name from seabed.cruise;"
            cursor.execute(SQL)
            #cursor.fetchall()
            for row in cursor:
               print(row)
            response = input("\nPlease enter a cruise_id from the above: ")
            cruise_id = (response.strip(),)
            #SQL="select dive. , from dive where dive.cruise_id= %s);"
            #cursor.execute(SQL, cruise_id)


         ### NUMBER OF EACH ORGANISM SUBYTYPEBY DIVE
         elif choice == 8:
            response = input("\nEnter the dive directory (e.g., d20150917_1): ")
            dive = (response.strip(),)
            SQL='SELECT DISTINCT fct.org_subtype as Subtype FROM seabed.fct WHERE fct.dive_id=(select id from dive where directory = %s) order by Subtype;'
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
 

         ### NUMBER OF FISHCOUNT (FCT) FILES BY DIVE
         elif choice == 9:
            ### TODO: should we report any empty fct files? (they have to get uploaded first, obviously, but seabed.py doesn't do this currently)
            response = input("\nEnter the dive directory (e.g., d20150917_1): ")
            dive = (response.strip(),)
            SQL = "SELECT COUNT(distinct fct.originating_fct) FROM fct WHERE fct.dive_id=(select id from dive where directory = %s);"
            cursor.execute(SQL, dive)
            for row in cursor:
               print(row[0])

            


         ### NUMBER OF IMAGES BY DIVE     
         elif choice == 10:
            response = input("\nPlease enter a dive directory (e.g., d20150917_1): ")
            dive_dir = (response.strip(),)
            SQL="select count(distinct fct.filename) from fct where fct.dive_id=(select id from dive where directory = %s);"
            cursor.execute(SQL, dive_dir)
            qreturn = cursor.fetchall()
            for row in qreturn:
               print(row[0])

  
         ### IMAGES WITH MORE THAN N ORGANISMS       
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

  
         ### PRODUCE ORGANISM HISTOGRAM BY DIVE   
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

           ### TODO: option to update the DB with names from the Species lookup table
           #print('Would you like to change a field in the database? [y/N]')

 

         ### DIVE AREA SURVEYED BY SUBSTRATE        
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

         
         ### PLOT CTD+OPTODE DATA (with QC stats)
         elif choice == 14:
            response = input("\nPlease enter a dive directory (e.g., d20150917_1): ")
            dive_dir = (response.strip(),)
            ### This gets the CTD data:
            SQL="select seabed.ctd.rovtime, seabed.ctd.dive_id, seabed.ctd.sal, seabed.ctd.temp, seabed.ctd.pres, seabed.ctd.sos from seabed.ctd where seabed.ctd.dive_id=(select seabed.dive.id from seabed.dive where seabed.dive.directory = %s);"
            cursor.execute(SQL, dive_dir)
            CTDqreturn = cursor.fetchall()

            ctd_times = [x[0] for x in CTDqreturn]
            ctd_sals = [x[2] for x in CTDqreturn]
            ctd_temps = [x[3] for x in CTDqreturn]
            ctd_depths = [x[4] for x in CTDqreturn]
            #ctd_soss = [x[5] for x in CTDqreturn]
            
            ### This gets the OPTODE data:
            SQL="select seabed.optode.rovtime, seabed.optode.psat, seabed.optode.conc from seabed.optode where seabed.optode.dive_id=(select seabed.dive.id from seabed.dive where seabed.dive.directory = %s);"
            cursor.execute(SQL, dive_dir)
            OPTqreturn = cursor.fetchall()

            ### Now, we need to associate psat and conc to a depth, via a close rovtime  
            opt_times = [x[0] for x in OPTqreturn]
            #opt_psats = [x[1] for x in OPTqreturn]
            opt_concs = [x[2] for x in OPTqreturn]
 
            ### I think this is the best way to get nearest ctd time to optode time
            # NOTE: opt_sals and opt_temps here are just subsets from ctd_[sals/temps] where the timestamp is closest
            opt_depths = []
            opt_sals = []
            opt_temps = []
            print("Calculating times, salinities, and temperatures for the optode...")
            for ot in opt_times:
               time_diff = np.abs([date - ot for date in ctd_times])
               min_idx = time_diff.argmin(0)
               opt_depths.append(ctd_depths[min_idx])
               opt_sals.append(ctd_sals[min_idx])
               opt_temps.append(ctd_temps[min_idx])


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
            #opt_psats_trunc = [val for idx, val in enumerate(opt_psats) if opt_depths_logic[idx] == True]
            opt_concs_trunc = [val for idx, val in enumerate(opt_concs) if opt_depths_logic[idx] == True]
            opt_sals_trunc = [val for idx, val in enumerate(opt_sals) if opt_depths_logic[idx] == True]
            opt_temps_trunc = [val for idx, val in enumerate(opt_temps) if opt_depths_logic[idx] == True]
            

            ### output some QA
            # TEMPERATURE
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

            # SALINITY
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

            # O2 -- NOTE!!! We're using opt_concs_trunc, NOT opt_psats_trunc. ALSO, opt_depths_trunc is fine; no need to convert to pressure
            out_O2s = []
            O2s_cal = []
            for idx in np.arange(0, len(opt_concs_trunc)):
               O2s_cal.append(cal_O2(opt_concs_trunc[idx], opt_temps_trunc[idx], opt_sals_trunc[idx], opt_depths_trunc[idx]))

            avg_O2s = stat.mean(O2s_cal)
            sd2_O2s = 2 * stat.stdev(O2s_cal)
            for O2 in O2s_cal:
               if O2 > avg_O2s + sd2_O2s:
                  out_O2s.append(O2)
               elif O2 < avg_O2s - sd2_O2s:
                  out_O2s.append(O2)
 
            if len(out_O2s) > 0:           
               print("\nO2 psats recorded more than 2 SD from the mean: \n mean: ", avg_O2s, "\n Anomalies: ", out_O2s)
               hist(O2s_cal,'O2 psat (calibrated)')        

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
            ax3.set_xlabel('Calibrated O2 (uM)', color=color)
            ax3.plot(O2s_cal, opt_depths_trunc, color=color, marker='.', linestyle='None')
            ax3.tick_params(axis='x', labelcolor=color, pad=30)
            
            plt.gca().invert_yaxis()
            plt.tight_layout()
            plt.show()




         ### EXPORT MERGED FCT FILES WITH CTD+OPTODE DATA
         elif choice == 15:
            response = input("\nPlease enter a dive directory (e.g., d20150917_1): ")
            dive_dir = (response.strip(),)
            output_file = input("\nPlease enter an output file: ")
            SQL="select * from seabed.fct where seabed.fct.dive_id=(select seabed.dive.id from seabed.dive where seabed.dive.directory = %s);"
            cursor.execute(SQL, dive_dir)
            FCTqreturn = cursor.fetchall()

            #### Recycling code from query 14
            ### This gets the CTD data:
            SQL="select seabed.ctd.rovtime, seabed.ctd.dive_id, seabed.ctd.sal, seabed.ctd.temp, seabed.ctd.pres, seabed.ctd.sos from seabed.ctd where seabed.ctd.dive_id=(select seabed.dive.id from seabed.dive where seabed.dive.directory = %s);"
            cursor.execute(SQL, dive_dir)
            CTDqreturn = cursor.fetchall()

            ctd_times = [x[0] for x in CTDqreturn]
            ctd_sals = [x[2] for x in CTDqreturn]
            ctd_temps = [x[3] for x in CTDqreturn]
            ctd_depths = [x[4] for x in CTDqreturn]
            #ctd_soss = [x[5] for x in CTDqreturn]
            
            ### This gets the OPTODE data:
            SQL="select seabed.optode.rovtime, seabed.optode.psat, seabed.optode.conc from seabed.optode where seabed.optode.dive_id=(select seabed.dive.id from seabed.dive where seabed.dive.directory = %s);"
            cursor.execute(SQL, dive_dir)
            OPTqreturn = cursor.fetchall()

            ### Now, we need to associate psat and conc to a depth, via a close rovtime 
            opt_times = [x[0] for x in OPTqreturn]
            #opt_psats = [x[1] for x in OPTqreturn]
            opt_concs = [x[2] for x in OPTqreturn]
 
            ### I think this is the best way to get nearest ctd time to optode time
            # NOTE: opt_sals and opt_temps here are just subsets from ctd_[sals/temps] where the timestamp is closest
            opt_depths = []
            opt_sals = []
            opt_temps = []
            print("Finding times, salinities, and temperatures for the optode...")
            for ot in opt_times:
               time_diff = np.abs([date - ot for date in ctd_times])
               min_idx = time_diff.argmin(0)
               opt_depths.append(ctd_depths[min_idx])
               opt_sals.append(ctd_sals[min_idx])
               opt_temps.append(ctd_temps[min_idx])

  
            ### Calibrate O2
            print("Calibrating O2...")
            out_O2s = []
            O2s_cal = []
            for idx in np.arange(0, len(opt_concs)):
               O2s_cal.append(cal_O2(opt_concs[idx], opt_temps[idx], opt_sals[idx], opt_depths[idx]))


            ### Next we need to get the CTD+OPTODE data for each depth returned by the FCT query. These will be lists of the same length as FCTqreturn
            fct_depths = [x[3] for x in FCTqreturn]

            fct_temps = []
            fct_sals = []
            fct_O2s = []
         
            print("Finding CTD+OPTODE for each FCT depth...")
            for f_depth in fct_depths:
               depth_diff = np.abs([f_depth - o_depth for o_depth in opt_depths])
               min_idx = depth_diff.argmin(0)
               fct_temps.append(opt_temps[min_idx])
               fct_sals.append(opt_sals[min_idx])
               fct_O2s.append(O2s_cal[min_idx])


            ### Make a list (to export to csv) of lists (each fct row, to which is appended CTD+OPTODE data)
            fcts_list = []
            for idx in np.arange(0, len(FCTqreturn)):
               fct_row = list(FCTqreturn[idx])
               fct_row.append(fct_temps[idx])
               fct_row.append(fct_sals[idx])
               fct_row.append(fct_O2s[idx])
               fcts_list.append(fct_row)


            ### Let's get the column names
            ### TODO: does this need 'seabed' prepended to fct?
            SQL = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'fct';"
            cursor.execute()
            cols = [cols[i][0] for i in np.arange(0, len(cols))]
            cols.append('temp')
            cols.append('salinity')
            cols.append('O2_calibrated')

            with open(output_file, "w", newline="") as f:
               writer = csv.writer(f)
               writer.writerow(cols)
               writer.writerows(fcts_list)



         ### EXPORT ENTIRE DIVE
         elif choice == 16:
            print("Not implemented yet")
            continue
            response = input("\nPlease enter a dive directory (e.g., d20150917_1): ")
            dive_dir = (response.strip(),)
            output_dir = input("\nPlease enter an output directory with write permissions: ")
            output_dir = os.path.join(output_dir, response)
            ### TODO: Get table names from the DB instead of hard-coding
            tables = ["","","",""]
            for tbl in tables:
               SQL="SELECT * FROM {} WHERE {}.dive_id=(select seabed.dive.id from seabed.dive where seabed.dive.directory = %s);".format(tbl, tbl)
               cursor.execute(SQL, dive_dir)
               qreturn = cursor.fetchall()
               ### Extract the table headers.
               headers = [i[0] for i in cursor.description]
               fileName = tbl + '.csv'
               csvFile = csv.writer(open(os.path.join(output_dir, fileName), 'w', newline=''),
                             delimiter=',', lineterminator='\r\n',
                             quoting=csv.QUOTE_ALL, escapechar='\\')

               ### Add the headers and data to the csvs file.
               csvFile.writerow(headers)
               csvFile.writerows(qreturn)
               

 
      
   finally:
      conn.close()
   
   return 0
   

if __name__ == "__main__":
   #parser = ArgumentParser(description="Select queries to perform against SeaBED database.")
   #parser.add_argument("-n", "--dbname", dest="dbname", default="seabed", help="Name of database")
   #parser.add_argument("-u", "--user", dest="user", default="seabed", help="user name")
   #parser.add_argument("-s", "--server", dest="host", default="nwcdbp24.nwfsc.noaa.gov", help="server name")
   #parser.add_argument("-p", "--port", dest="port", default="5455", help="port at host")
   #parser.add_argument("-w", "--password", dest="password", default="", help="db password for this user")

   ### Example: python queries.py -n seabed -u paulr -s nwcdbp24.nwfsc.noaa.gov -p 5455 -w [your password]
   #args = parser.parse_args()
   sys.exit(main())
