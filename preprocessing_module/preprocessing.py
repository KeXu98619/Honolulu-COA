import os
import sys
import yaml
import pandas as pd
import numpy as np

# Set path to enable module imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# Set up logger
import utility_module.logger as logging 
logger = logging.get_logger('__name__','../../log_files/preprocessing.log')


class HoloData:
	"""
	Class that stores Holocard data & associated methods. 
	Methods: 
		- __init__()               : Read in Holo Data, config file, GTFS data
		- normalize()              : Column renaming & creation, value aliasing & type-casting, NA-handling
		- recode_stop_ids()        : Re-code stop_number_holo to match GTFS & APC. 
		- identify_valid_records() : Tag record as valid/invalid based on criteria from config
		- write_tables()           : Write/update tables 
	"""

	def __init__(self):

		# Config File
		try:
			with open(os.path.join("config_files", "config_HOLO_processing.yaml"), "r") as f:
				self.config = yaml.safe_load(f)
		except:
			logger.exception("Could not read HOLO config file")

		# Holo Data
		try:
			logger.info("Reading HOLO data...")
			self.df = pd.concat([pd.read_csv(os.path.join(self.config['holo_data_dir'], file), skiprows=3) for file in os.listdir(self.config['holo_data_dir'])])
		except: 
			logger.exception("Could not read HOLO Data.")

		# GTFS tables
		try:
			logger.info("Reading GTFS data...")
			self.gtfs_feed_info = pd.read_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_feed_info.csv'), parse_dates=['service_day'])
			self.gtfs_stops = pd.read_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_stops.csv'))
			self.gtfs_routes = pd.read_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_routes.csv'))

			# Cast service_date as 'date' rather than 'datetime' to match Holo
			self.gtfs_feed_info['service_day'] = self.gtfs_feed_info['service_day'].dt.date
		except: 
			logger.exception("Could not read GTFS Data.")


	def normalize(self):
		"""
		1. Column renaming
		2. NA handling
		3. Value aliasing
		4. Create additional columns
		5. Data type casting

		Questions: 
			- Keep only relevant columns here? 
		"""

		logger.info('Normalizing HOLO Dataset')

		## 1. Column renaming
		logger.info('\tRenaming columns')
		self.df.columns = self.df.columns.map(self.config['column_aliases'])

		## 2. NA handling
		logger.info('\tNA Handling')
		for col, fillval in self.config['na_filling'].items():
			self.df[col] = self.df[col].fillna(fillval)

		## 3. Value aliasing
		logger.info('\tValue Aliasing')
		for col, mapping in self.config['column_lookup_tables'].items():
		    try: 
		        assert len(set(self.df[col].unique()) - set(self.config['column_lookup_tables'][col].keys()))==0
		        self.df[col] = self.df[col].map(mapping)
		    except: 
		        logger.exeption(f"Error value aliasing column {col}.")

		## 4. Create additional columns
		logger.info('\tCreating additional columns')

		# (a) tap_datetime & closeout_datetime
		self.df['tap_datetime'] = pd.to_datetime(self.df['tap_date'] + ' ' + self.df['tap_time'])
		self.df['closeout_datetime'] = pd.to_datetime(self.df['closeout_date'])

		# (b) service_day
		self.df['service_day'] = self.df['tap_datetime'].dt.date
		criteria = self.df['tap_datetime'].dt.hour.isin([0, 1, 2])
		self.df.loc[criteria, 'service_day'] = self.df.loc[criteria, 'service_day'] - pd.Timedelta(days=1)

		## 5. Data type casting
		logger.info('\tData typecasting')
		# a. Columns requiring special pre-processing before casting
		self.df['ride_amount'] = self.df['ride_amount'].str.replace('$', '', regex=False)
		self.df['transaction_uid'] = self.df['transaction_uid'].str.replace(',','')
		# b. Set dtypes
		for col, dtype in self.config['column_dtypes'].items():
			self.df[col] = self.df[col].astype(dtype)


	def recode_gtfs_ids(self):
		"""
		Recode 'stop_id' & 'route_id' to match GTFS format. 
		"""
		
		# Add GTFS feed to use
		self.df = self.df.merge(self.gtfs_feed_info, on=['service_day'], how='left')

		# Drop rows with service day outside range of gtfs_feed
		self.df = self.df[~self.df['gtfs_feed'].isna()].reset_index(drop=True)

		### stop_id

		# Adjust Stop ID coding
		self.df['stop_number_holo'] = self.df['stop_number_holo'] % 1000000

		# Code NA in the same manner as APC
		self.df.loc[self.df['stop_number_holo'].isin([209999, 999999]), 'stop_number_holo'] = 9999

		# Add GTFS stop_code
		self.df = self.df.merge(
				self.gtfs_stops[['gtfs_feed', 'stop_code', 'stop_serial_number']], 
				left_on=['gtfs_feed', 'stop_number_holo'], right_on=['gtfs_feed', 'stop_serial_number'], how='left') \
		    .drop(columns=['stop_serial_number'])

		# Fill missing stop_codes
		self.df['stop_code'] = self.df['stop_code'].fillna(9999).astype(int)

		# Log Stop IDs in Holo that are not found in GTFS
		logger.info(f"Records missing stop_id: {(self.df['stop_code']==9999).sum():,} ({(self.df['stop_code']==9999).sum()/self.df.shape[0]:.2%})")

		### route_id

		# Adjust Route ID coding
		route_mapping = {}
		for route_id in self.df['route_number_holo'].unique():
		    route_id_cleaned = route_id[2:]
		    try:
		        route_id_cleaned = str(int(route_id_cleaned))
		    except:
		        route_id_cleaned = route_id_cleaned.lstrip("0")
		    route_mapping[route_id] = route_id_cleaned
		self.df['route_number_holo'] = self.df['route_number_holo'].map(route_mapping)

		# Add GTFS route_id
		self.df = self.df.merge(self.gtfs_routes[['gtfs_feed', 'route_id', 'route_short_name']], 
				left_on=['gtfs_feed', 'route_number_holo'], right_on=['gtfs_feed', 'route_short_name'], how='left') \
		    .drop(columns=['route_short_name'])

		# Fill missing stop_codes
		self.df['route_id'] = self.df['route_id'].fillna(9999).astype(int)

		# Log Stop IDs in Holo that are not found in GTFS
		logger.info(f"Records missing route_id: {(self.df['route_id']==9999).sum():,} ({(self.df['route_id']==9999).sum()/self.df.shape[0]:.2%})")


	def identify_valid_records(self):
		"""
		Label each record as valid/invalid. If invalid, give reason. 

		Reasons for invalidity:
			1. Duplicate Transaction UID.
			2. Tap after x seconds of tap by same Holocard. 
			3. Missing stop_code or route_id
		"""

		logger.info('Identifying valid records')
		
		# Initialize
		self.df['valid'] = 1
		self.df['invalid_reason'] = -1

		## (1) Duplicate Transaction UID

		invalid = list(self.df['transaction_uid'].value_counts() \
		    [self.df['transaction_uid'].value_counts() > 1].index)
		self.df.loc[self.df['transaction_uid'].isin(invalid), 'valid'] = 0
		self.df.loc[self.df['transaction_uid'].isin(invalid), 'invalid_reason'] = 1

		## (2) Tap after x seconds of tap by same Holocard 

		# Sort by holocard_id, tap_datetime
		self.df = self.df.sort_values(['holocard_uid', 'tap_datetime']).reset_index(drop=True)

		# Calculate seconds since last tap
		self.df['secs_since_last_tap'] = (self.df['tap_datetime'] - self.df['tap_datetime'].shift()).dt.seconds

		# Set 'secs_since_last_tap' of first tap to be nan
		self.df['idx'] = range(self.df.shape[0])
		self.df.loc[self.df['idx'].isin(self.df.groupby(['holocard_uid']).agg({'idx':'min'}).reset_index()['idx']), 'secs_since_last_tap'] = np.nan

		# Ensure all holocard taps are accounted for
		assert self.df['secs_since_last_tap'].isna().sum()==self.df['holocard_uid'].nunique()

		# Mark as invalid
		criteria = self.df['secs_since_last_tap']<self.config['double_tap_threshold_seconds']
		self.df.loc[criteria, 'valid'] = 0 
		self.df.loc[criteria, 'invalid_reason'] = 2

		self.df.drop(columns=['idx', 'secs_since_last_tap'], inplace=True)

		## (3) Missing stop_code or route_id
		self.df.loc[self.df['stop_code']==9999, 'valid'] = 0
		self.df.loc[self.df['stop_code']==9999, 'invalid_reason'] = 3
		self.df.loc[self.df['route_id']==9999, 'valid'] = 0
		self.df.loc[self.df['route_id']==9999, 'invalid_reason'] = 3

		# Log update
		logger.info(f"{(self.df['valid']==0).sum():,}/{self.df.shape[0]:,} invalid records ({(self.df['valid']==0).sum()/self.df.shape[0]:.2%})")


	def write_tables(self):
		"""
		Create tables if necessary. Else, append new records. 
			1. All Records Table
			2. Valid Records Table
			3. Invalid Records Table
			4. Lookup Tables
		"""

		logger.info('Writing tables...')
		
		## 1. Holo Table
		self.df.to_csv(os.path.join(self.config['out_dir'], 'holo_processed.csv'), index=None)

		## 2. Lookup Tables

		# Create lookup table for each attribute whose values were aliased
		for name, mapping in self.config['column_lookup_tables'].items():

		    # Create lookup table
		    lookup_table = pd.DataFrame()
		    lookup_table[name] = mapping.values()
		    lookup_table[name.replace('_id', '_name')] = mapping.keys()

		    # Write Out
		    lookup_table.to_csv(os.path.join(self.config['out_dir'], 'lookup', f"lookup_{name}.csv"), index=None)



class APCData:
	"""
	Class that stores APC data & associated methods. 
	Methods: 
		- __init__()               : Read in APC data, Config file
	"""

	def __init__(self):

		# Config File
		try:
			with open(os.path.join('config_files', 'config_APC_processing.yaml'), "r") as f:
				self.config = yaml.safe_load(f)
		except:
			logger.exception("Could not read APC config file")

		# APC Data
		try:
			self.df = pd.concat([pd.read_csv(os.path.join(self.config['apc_data_dir'], file)) for file in os.listdir(self.config['apc_data_dir']) if file.endswith('.csv')])
		except: 
			logger.exception("Could not read APC Data.")


	def normalize(self):
		"""
		1. Keep & rename relevant columns
		2. Change data types
		3. Create attributes
		4. Handle NA

		"""

		## 1. Keep & rename relevant columns
		self.df = self.df[self.config['column_aliases'].keys()].rename(columns=self.config['column_aliases'])

		## 2. Change data types
		self.df['stop_time'] = pd.to_datetime(self.df['stop_time'])
		self.df['sched_time'] = pd.to_datetime(self.df['sched_time'])

		## 3. Create attributes

		# Service Day
		self.df['service_day'] = self.df['stop_time'].dt.date
		criteria = self.df['stop_time'].dt.hour.isin([0, 1, 2])
		self.df.loc[criteria, 'service_day'] = self.df.loc[criteria, 'service_day'] - pd.Timedelta(days=1)

		## 4. Handle NA

		# Determine missing sched_time
		self.df.loc[self.df['sched_time'].dt.year==1980, 'sched_time'] = np.nan


	def recode_gtfs_ids(self):
		"""
		(1) Route ID
			- Recode route_id using lookup file. 
			- Why can't we just use GTFS? A large number of APC route_ids are not accounted for in gtfs_routes. 
			- This route_id lookup table was created with the client. 
			- Notice that because we're using this static lookup table, the route_ids are agnostic of GTFS Period. 
		(2) Stop Code
			- Can use GTFS files alone to encode
		"""

		### (1) route_id

		gtfs_routes = pd.read_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_routes.csv')) \
		    [['route_id', 'route_short_name']].drop_duplicates().rename(columns={'route_id':'route_id', 'route_short_name':'route_name'})
		route_lookup = pd.read_csv(self.config['route_lookup']) \
		    [['ROUTE', 'ROUTE_NAME']].rename(columns={'ROUTE':'route_id_apc', 'ROUTE_NAME':'route_name'})

		# Mapping from APC route_id to GTFS route_id
		route_mapping = gtfs_routes.merge(route_lookup, on='route_name', how='right').drop(columns='route_name')

		# Add GTFS route_id to dataframe
		self.df = self.df.merge(route_mapping, on='route_id_apc', how='left')

		# Fill NA
		self.df['route_id'] = self.df['route_id'].fillna(9999).astype(int)


		### (2) stop_code

		gtfs_feed_info = pd.read_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_feed_info.csv'), parse_dates=['service_day'])
		gtfs_feed_info['service_day'] = gtfs_feed_info['service_day'].dt.date
		gtfs_stops = pd.read_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_stops.csv'))

		# Determine GTFS Feed to use
		self.df = self.df.merge(gtfs_feed_info, on='service_day', how='left')

		# Drop rows with service day outside range of gtfs_feed
		self.df = self.df[~self.df['gtfs_feed'].isna()].reset_index(drop=True)

		# Encode using GTFS stop_id
		self.df = self.df.merge(gtfs_stops[['gtfs_feed', 'stop_code']].rename(columns={'stop_id':'stop_code'}), 
		         left_on=['gtfs_feed', 'stop_id_apc'], right_on=['gtfs_feed', 'stop_code'], how='left')

		# Fill NA with 9999
		self.df['stop_code'] = self.df['stop_code'].fillna(9999).astype(int)


	def identify_valid_records(self):
		
		# Initialize
		self.df['valid'] = 1

		# Any record with missing stop_code or route id is invalid
		self.df.loc[(self.df['stop_code']==9999) | (self.df['route_id']==9999), 'valid'] = 0

		# Print update
		logger.info(f"APC Valid Records")
		logger.info(f"\tRows: {self.df[self.df['valid']==1].shape[0]:,}/{self.df.shape[0]:,} ({self.df[self.df['valid']==1].shape[0]/self.df.shape[0]:.2%})")
		logger.info(f"\tBoardings: {self.df[self.df['valid']==1]['boardings'].sum():,}/{self.df['boardings'].sum():,} ({self.df[self.df['valid']==1]['boardings'].sum()/self.df['boardings'].sum():.2%})")
		

	def write_tables(self):
		logger.info('Writing APC table...')
		self.df.to_csv(os.path.join(self.config['out_dir'], 'apc_processed.csv'), index=None)



def main():

	# HOLO
	holo = HoloData()
	holo.normalize()
	holo.recode_gtfs_ids()
	holo.identify_valid_records()
	holo.write_tables()

	# APC
	apc = APCData()
	apc.normalize()
	apc.recode_gtfs_ids()
	apc.identify_valid_records()
	apc.write_tables()


if __name__ == "__main__":
	main()