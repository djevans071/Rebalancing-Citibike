CREATE TABLE stations (
	id INTEGER NOT NULL PRIMARY KEY, 
	docks INTEGER NOT NULL, 
	lat FLOAT NOT NULL, 
	long FLOAT NOT NULL, 
	name VARCHAR(45) NOT NULL
);
alter table stations owner to psam071;

CREATE TABLE features (
	id FLOAT NOT NULL, 
	date DATE NOT NULL, 
	hour INTEGER NOT NULL, 
	bikes_out FLOAT NOT NULL, 
	bikes_in FLOAT NOT NULL,
	dayofweek FLOAT NOT NULL, 
	month FLOAT NOT NULL, 
	year FLOAT NOT NULL, 
	is_weekday VARCHAR(5) NOT NULL, 
	is_holiday VARCHAR(5) NOT NULL, 
	rbikes_out FLOAT NOT NULL, 
	rbikes_in FLOAT NOT NULL,
	rebal_net_flux FLOAT NOT NULL, 
	tot_docks FLOAT NOT NULL, 
	avail_bikes FLOAT NOT NULL, 
	avail_docks FLOAT NOT NULL,
	row_id serial NOT NULL,
	CONSTRAINT features_pkey PRIMARY KEY (row_id)
);
alter table features owner to psam071;

CREATE TABLE weather (
	index int NOT NULL PRIMARY KEY,
	date DATE NOT NULL,
	hour INTEGER NOT NULL,
	precip FLOAT NOT NULL,
	snow INTEGER NOT NULL,
	temp FLOAT NOT NULL
);
alter table weather owner to psam071;

SELECT a.id, a.date, a.hour, a.bikes_out, a.bikes_in, dayofweek, month, is_weekday, is_holiday, tot_docks, avail_bikes, avail_docks
INTO features_subset FROM features a
RIGHT JOIN unbal_stations b ON a.id = b.id;

