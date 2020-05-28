CREATE TABLE cruise (
	id SERIAL NOT NULL PRIMARY KEY,
	vehicle_name TEXT,
	vehicle_cfg TEXT,
	cruise_name TEXT,
	cruise_id TEXT,
	ship_name TEXT,
	chief_sci TEXT,
	UNIQUE(vehicle_name, cruise_id)
);


CREATE TABLE dive (
	id SERIAL NOT NULL PRIMARY KEY,
	cruise_id INTEGER NOT NULL REFERENCES cruise,
	directory TEXT NOT NULL,
        filename TEXT NOT NULL,
	filetime TIMESTAMP NOT NULL,
	starttime TIMESTAMP NOT NULL,
	endtime TIMESTAMP NOT NULL,
	ready BOOLEAN NOT NULL,
	location TEXT,
	origin_lat DOUBLE PRECISION,
	origin_lon DOUBLE PRECISION,
	utm_zone TEXT,
	utm_x TEXT,
	utm_y TEXT,
	mag_variation TEXT,
	
	UNIQUE(cruise_id, filename)
);

-- used to have: 'period DOUBLE PRECISION,' (fromthe syscfg file?), but I think we should really compute this on the query side. My files show rovtime as the first field
CREATE TABLE camera (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	image_directory TEXT,
	PRIMARY KEY(dive_id, rovtime, image_directory)
);

CREATE TABLE deltaT (
	dive_id INTEGER NOT NULL REFERENCES dive,
	log_dir TEXT,
	PRIMARY KEY(dive_id, log_dir)
);

CREATE TABLE blueview (
	dive_id INTEGER NOT NULL REFERENCES dive,
	log_dir TEXT,
	PRIMARY KEY(dive_id, log_dir)
);

CREATE TABLE logger (
	dive_id INTEGER NOT NULL REFERENCES dive,
	log_dir TEXT,
	PRIMARY KEY(dive_id, log_dir)
);

CREATE TABLE traj (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	depth DOUBLE PRECISION,
	depth_vel DOUBLE PRECISION,
	heading DOUBLE PRECISION,
	heading_vel DOUBLE PRECISION,
	surge_speed DOUBLE PRECISION,
	surge_acc DOUBLE PRECISION,
	depth_goal DOUBLE PRECISION,
	heading_goal DOUBLE PRECISION,
	surge_speed_goal DOUBLE PRECISION,
	sway_speed DOUBLE PRECISION,
	sway_acc DOUBLE PRECISION,
	sway_speed_goal DOUBLE PRECISION,
	PRIMARY KEY(dive_id, rovtime)
);

-- there are intregity violations with this table
CREATE TABLE est (
	dive_id  INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	x_lbl DOUBLE PRECISION,
	y_lbl DOUBLE PRECISION,
	x_dop DOUBLE PRECISION,
	y_dop DOUBLE PRECISION,
	x_dop_lbl DOUBLE PRECISION,
	y_dop_lbl DOUBLE PRECISION,
	x_ctl DOUBLE PRECISION,
	y_ctl DOUBLE PRECISION,
	vx DOUBLE PRECISION,
	vy DOUBLE PRECISION,
	vz DOUBLE PRECISION,
	depth DOUBLE PRECISION,
	depth_rate DOUBLE PRECISION,
	altitude DOUBLE PRECISION,
	heading DOUBLE PRECISION,
	heading_rate DOUBLE PRECISION,
	roll DOUBLE PRECISION,
	pitch DOUBLE PRECISION,
	PRIMARY KEY(dive_id, rovtime)
);

CREATE TABLE goal (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	goal_id INTEGER NOT NULL,
	goal_str TEXT,
	xpos1 DOUBLE PRECISION,
	ypos1 DOUBLE PRECISION,
	zpos1 DOUBLE PRECISION,
	xpos2 DOUBLE PRECISION,
	ypos2 DOUBLE PRECISION,
	zpos2 DOUBLE PRECISION,
	heading DOUBLE PRECISION,
	xy_vel DOUBLE PRECISION,
	z_vel DOUBLE PRECISION,
	PRIMARY KEY(dive_id, rovtime, goal_id)
);

CREATE TABLE thr (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	port DOUBLE PRECISION,
	stbd DOUBLE PRECISION,
	vert DOUBLE PRECISION,
	lat DOUBLE PRECISION,
	PRIMARY KEY(dive_id, rovtime)
);

-- there are intregity violations with this table
CREATE TABLE pxf (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	imgname TEXT,
	imgnum INTEGER,
	PRIMARY KEY(dive_id, rovtime)
);	
	
CREATE TABLE rdi (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	altitude DOUBLE PRECISION,
	r1 DOUBLE PRECISION,
	r2 DOUBLE PRECISION,
	r3 DOUBLE PRECISION,
	r4 DOUBLE PRECISION,
	PRIMARY KEY(dive_id, rovtime)
);

-- the depth field specifies which of saltwater or fresh is being used. Right now we just copy msw to depth.
CREATE TABLE paro (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	msw DOUBLE PRECISION,
	mfw DOUBLE PRECISION,
	depth DOUBLE PRECISION,
	PRIMARY KEY(dive_id, rovtime)
);

CREATE TABLE battery (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	avg_charge REAL,
	total_current REAL,
	total_power REAL,
	volts_in REAL,
	volts_out REAL,
	cycles INTEGER,
	time_to_empty REAL,
	PRIMARY KEY(dive_id, rovtime)
);

-- note: the following three 'thr' tables could be combined into one, but keeping 
-- the original model of the data (and a thr table already exists)

CREATE TABLE thr_vert (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	speed DOUBLE PRECISION,
	current DOUBLE PRECISION,
	voltage DOUBLE PRECISION,
	temp DOUBLE PRECISION,
	PRIMARY KEY(dive_id, rovtime)
);

CREATE TABLE thr_port (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	speed DOUBLE PRECISION,
	current DOUBLE PRECISION,
	voltage DOUBLE PRECISION,
	temp DOUBLE PRECISION,
	PRIMARY KEY(dive_id, rovtime)
);

CREATE TABLE thr_stbd (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	speed DOUBLE PRECISION,
	current DOUBLE PRECISION,
	voltage DOUBLE PRECISION,
	temp DOUBLE PRECISION,
	PRIMARY KEY(dive_id, rovtime)
);

CREATE TABLE ctd (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	cond DOUBLE PRECISION,
	temp DOUBLE PRECISION,
	sal DOUBLE PRECISION,
	pres DOUBLE PRECISION,
	sos DOUBLE PRECISION,
	PRIMARY KEY(dive_id, rovtime)
);

CREATE TABLE optode (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	temp DOUBLE PRECISION,
	psat DOUBLE PRECISION,
	conc DOUBLE PRECISION,
	PRIMARY KEY(dive_id, rovtime)
);

CREATE TABLE octans (
	dive_id INTEGER NOT NULL REFERENCES dive,
	rovtime TIMESTAMP NOT NULL,
	heading DOUBLE PRECISION,
	pitch DOUBLE PRECISION,
	roll DOUBLE PRECISION,
	hr DOUBLE PRECISION,
	pr DOUBLE PRECISION,
	rr DOUBLE PRECISION,
	acc_x DOUBLE PRECISION,
	acc_y DOUBLE PRECISION,
	acc_z DOUBLE PRECISION,
	heave DOUBLE PRECISION,
	PRIMARY KEY(dive_id, rovtime)
);

CREATE TABLE fct (
	dive_id INTEGER NOT NULL REFERENCES dive, 
	latitude DOUBLE PRECISION,
	longitude DOUBLE PRECISION,
	depth DOUBLE PRECISION,
	filename TEXT,
	time TIMESTAMP,
	img_area DOUBLE PRECISION,
	img_width INTEGER,
	img_height INTEGER,
	substrate TEXT,
	org_type TEXT,
	org_subtype TEXT,
	index TEXT,
	org_x DOUBLE PRECISION,
	org_y DOUBLE PRECISION,
	org_length DOUBLE PRECISION,
	org_area DOUBLE PRECISION,
	comment TEXT,
	PRIMARY KEY(dive_id, time, org_x, org_y)
);

CREATE TABLE frames (
	dive_id INTEGER NOT NULL REFERENCES dive,
	deployment_ID TEXT,
	frame_number INTEGER,
	frame_time TEXT,
	primary_habitat_type TEXT,
	secondary_habitat_type TEXT,
	B1_present TEXT,
	B1_coverage NUMERIC,
	B2_present TEXT,
	B2_coverage NUMERIC,
	B3_present TEXT,
	B3_coverage NUMERIC,
	B4_present TEXT,
	B4_coverage NUMERIC,
	B5_present TEXT,
	B5_coverage NUMERIC,
	A1_checked TEXT,
	A2_checked NUMERIC,
	comment TEXT
);

CREATE TABLE targets (
	dive_id INTEGER NOT NULL REFERENCES dive,
	deployment_ID TEXT,
	frame_number INTEGER,
	target_number NUMERIC,
	class TEXT,
	LX NUMERIC,
	LY NUMERIC,
	RX NUMERIC,
	RY NUMERIC,
	Length NUMERIC,
	Range NUMERIC,
	Error NUMERIC,
	LHX NUMERIC,
	LHY NUMERIC,
	LTX NUMERIC,
	LTY NUMERIC,
	RHX NUMERIC,
	RHY NUMERIC,
	RTX NUMERIC,
	RTY NUMERIC,
	hx NUMERIC,
	hy NUMERIC,
	hz NUMERIC,
	tx NUMERIC,
	ty NUMERIC,
	tz NUMERIC,
	target_link INTEGER,
	comment TEXT
);
