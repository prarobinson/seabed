import sys
import psycopg2
#from worms_suds import *
from matplotlib import pyplot as plt
from argparse import ArgumentParser

def main(dbname, user):
	conn = psycopg2.connect("dbname=%s user=%s" % (dbname, user))
	try:
		cursor = conn.cursor()
		
		prompt = "Select from the following options (or q to quit):\n\n"
		prompt += "\t 1) Show all cruise information\n"
		prompt += "\t 2) Show all dive information\n"
		prompt += "\t 3) Show all organism types\n"
		prompt += "\t 4) Show all organism subtypes\n"
		prompt += "\t 5) Show all organism subtypes by dive\n"
		prompt += "\t 6) Number of fishcount files by dive\n"
		prompt += "\t 7) Number of images by dive\n"
		prompt += "\t 8) Images with with more than N organism(s) identified\n"
		prompt += "\t 9) Produce an organism count histogram\n"
		prompt += "\t10) Dive area surveyed by substrate\n"
		prompt += "\t11) Plot CTD data\n"
		prompt += "\t12) Logistical information for dives by cruise\n"
		prompt += "\t13) Dump database as .csv files\n"
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
				cursor.execute("SELECT DISTINCT org_type FROM fct WHERE org_type <> '' ORDER BY org_type")
				results = []
				for row in cursor:
					results.append(row[0])
				print(", ".join(results))
				
			elif choice == 4:
				cursor.execute("SELECT DISTINCT org_subtype FROM fct WHERE org_subtype <> '' ORDER BY org_subtype")
				results = []
				for row in cursor:
					results.append(row[0])
				print(", ".join(results))
				
			elif choice == 5:
				response = input("\nEnter the dive directory (e.g., d20150917_1): ")
				dive = response.strip()
				
				cursor.execute("SELECT DISTINCT org_subtype FROM fct, dive WHERE dive.id = fct.dive_id AND dive.directory = %s AND org_subtype <> '' ORDER BY org_subtype", (dive, ))
				results = []
				for row in cursor:
					results.append(row[0])
				print(", ".join(results))
				
			elif choice == 6:
				response = input("\nEnter the dive directory (e.g., d20150917_1): ")
				dive = response.strip()
				
				cursor.execute("SELECT COUNT(*) FROM fct, dive WHERE dive.id = fct.dive_id AND dive.directory = %s", (dive, ))
				for row in cursor:
					print(row[0])
					
			elif choice == 7:
				response = input("\nPlease enter a dive directory (e.g., d20150917_1): ")
				### To avoid injection attacks, it is recommended to let cursor.execute() handle the string variable in SQL commands
				dive_dir = (response.strip(),)
				SQL="select count(distinct fct.filename) from fct where fct.dive_id=(select id from dive where directory = %s);"
				cursor.execute(SQL, dive_dir)
				qreturn = cursor.fetchall()

				for row in qreturn:
					print(row[0])
			
			elif choice == 8:
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
			
			elif choice == 9:
				counts = {0: 0}
				count = 0
				prev = None
				
				cursor.execute("SELECT filename, org_type, org_subtype, comment FROM fct ORDER BY filename")
				
				for row in cursor:
					orgtype = row[1]
					comment = row[3]
					if orgtype.lower() == "other" and comment.lower() == "no fish":
						counts[0] += 1
						
					else:
						filename = row[0]
						if filename == prev:
							count += 1
						
						elif count > 0:
							try:
								counts[count] += 1
							except:
								counts[count] = 1
								
							count = 1
						
						prev = filename
				print(counts)
			
			elif choice == 10:
				response = input("\nPlease enter a dive directory (e.g., d20150917_1): ")
				### To avoid injection attacks, it is recommended to let cursor.execute() handle the string variable in SQL commands
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
				#print(types_areas)
			
			elif choice == 11:
				response = input("\nPlease enter a dive directory (e.g., d20150917_1): ")
				### To avoid injection attacks, it is recommended to let cursor.execute() handle the string variable in SQL commands
				dive_dir = (response.strip(),)
				### This gets the CTD data:
				SQL="select ctd.rovtime, ctd.dive_id, ctd.sal, ctd.temp, ctd.pres, ctd.sos from ctd where ctd.dive_id=(select id from dive where directory = %s);"
				cursor.execute(SQL, dive_dir)
				CTDqreturn = cursor.fetchall()
				
				sals = [x[2] for x in CTDqreturn]
				temps = [x[3] for x in CTDqreturn]
				depths = [x[4] for x in CTDqreturn]
				soss = [x[5] for x in CTDqreturn]
				
				### We should truncate data from depths less than 1m
				clipped_depths_ind = []
				for index, value in enumerate(depths):
					if value > 1:
						clipped_depths_ind.append(index)
				dmin = min(clipped_depths_ind)
				dmax = max(clipped_depths_ind)
				
				depths_trunc = depths[dmin:dmax]
				temps_trunc = temps[dmin:dmax]
				sals_trunc = sals[dmin:dmax]
				
				fig, ax1 = plt.subplots()
				
				color = 'tab:green'
				ax1.set_ylabel('Depth (m)')
				color = 'blue'
				ax1.set_xlabel('Temp (C)', color=color)
				ax1.plot(temps_trunc, depths_trunc, color=color)
				ax1.tick_params(axis='x', labelcolor=color)
				
				ax2 = ax1.twiny()
				color = 'green'
				ax2.set_xlabel('Salinity (psu)', color=color)
				ax2.plot(sals_trunc, depths_trunc, color=color)
				ax2.tick_params(axis='x', labelcolor=color)
				
				### Gotta add O2 from 'OPTODE' lines in RAW.AUV
				#ax3 = ax1.twiny()
				#color = 'red'
				#ax3.set_xlabel('O2 (% saturation)', color=color)
				#ax3.plot(O2s, O2_depths, color=color)
				#ax3.tick_params(axis='x', labelcolor=color)
				
				plt.gca().invert_yaxis()
				plt.show()
				
				
				
				
			elif choice == 12:
				SQL="select cruise.id, cruise.cruise_name from cruise;"
				cursor.execute(SQL)
				#cursor.fetchall()
				for row in cursor:
					print(row)
				response = input("\nPlease enter a cruise_id from the above: ")
				cruise_id = (response.strip(),)
				print('This function is not implemented yet...')
				#SQL="select dive. , from dive where dive.cruise_id= %s);"
				#cursor.execute(SQL, cruise_id)



			elif choice == 13:
				response = input("\nPlease enter an output directory with write permissions: ")
				output_dir = (response.strip(),)
				SQL="CREATE OR REPLACE FUNCTION db_to_csv(path TEXT) RETURNS void AS $$ declare tables RECORD;   statement TEXT; begin FOR tables IN SELECT (table_schema || '.' || table_name) AS schema_table FROM information_schema.tables t INNER JOIN information_schema.schemata s ON s.schema_name = t.table_schema WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema') AND t.table_type NOT IN ('VIEW') ORDER BY schema_table LOOP statement := 'COPY ' || tables.schema_table || ' TO ''' || path || '/' || tables.schema_table || '.csv' ||''' DELIMITER '';'' CSV HEADER'; EXECUTE statement; END LOOP; return; end; $$ LANGUAGE plpgsql; SELECT db_to_csv(%s);"
				cursor.execute(SQL, output_dir)
		
	finally:
		conn.close()
	
	return 0
	

if __name__ == "__main__":
	parser = ArgumentParser(description="Select queries to perform against SeaBED database.")
	parser.add_argument("-n", "--dbname", dest="dbname", default="seabed", help="Name of database")
	parser.add_argument("-u", "--user", dest="user", default="seabed", help="Name of database user")
	### Example: python queries3x.py -n seabed -u paulr
	args = parser.parse_args()
	sys.exit(main(args.dbname, args.user))
