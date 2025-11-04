import yaml
import pandas as pd
import sys
import numpy as np
import os
import time
import geopandas as gpd

# Set path to enable module imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# Set up logger
import utility_module.logger as logging 
logger = logging.get_logger('__name__','../../log_files/expansion_module.log')


class ExpansionModule:
	"""
	Expand the processed HOLO linked trips module to the APC totals. 
	Methods: 
		- __init__()			   				: Read in processed config file, data
		- preprocess()			   				: (1) Keep only valid records (2) add helper columns
		- determine_scheduled_trip_start_time()	: Record the first trip sched_time that is available as the trip_sched_start_time
		- expansion()							: Implement expansion.
		- impute_holo_combos()					: Impute {'month', 'gtfs_feed', 'dow', 'tod', 'stop_code'} combinations that exist in HOLO but not in APC
		- write_table()							: Write Expanded Linked Trips table. 
	"""

	def __init__(self):

		## Config
		try:
			with open(os.path.join("config_files", "config_expansion.yaml"), "r") as f:
				self.config = yaml.safe_load(f)
		except:
			logger.exception("Could not read config file")

		## APC
		try:
			logger.info("Reading Processed APC data...")
			self.apc = pd.read_csv(os.path.join(self.config['out_dir'], 'apc_processed.csv'), parse_dates=['stop_time', 'sched_time', 'service_day'])
		except: 
			logger.exception("Could not read APC Data.")

		## HOLO
		try:
			logger.info("Reading Processed HOLO data...")
			self.holo = pd.read_csv(os.path.join(self.config['out_dir'], 'holo_linked_trips.csv'), parse_dates=['tap_datetime', 'service_day'])
		except: 
			logger.exception("Could not read HOLO Data.")

		## GTFS
		try:
			logger.info("Reading Processed GTFS data...")
			self.gtfs_stop_to_stop_dist = pd.read_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_stop_to_stop_dist.csv'))
		except: 
			logger.exception("Could not read GTFS Data.")

		## Initialize Expanded Linked Trips df
		self.expanded_linked_trips = None

	def preprocess(self):
		"""
		Preprocess data

		APC
			1. Keep only valid records
			2. Add helper columnms
		HOLO
			1. Add helper columns
			2. Convert to linked trips
		"""

		logger.info("Preprocessing...")


		### APC

		## 1. Keep only valid records
		self.apc = self.apc[self.apc['valid']==1].reset_index(drop=True)

		## 2. Add helper columns

		# Hour to TOD Mapping
		tod_mapping = {}
		for name, hours in self.config['tod_def'].items():
		    for hour in hours: 
		        tod_mapping[hour] = name
		        
		# Helper columns
		self.apc['month'] = self.apc['service_day'].dt.month
		self.apc['dow'] = self.apc['service_day'].dt.dayofweek
		self.apc['hour'] = self.apc['stop_time'].dt.hour
		self.apc['tod'] = self.apc['hour'].map(tod_mapping)

		### HOLO

		## 1. Add helper columns
		self.holo['month'] = self.holo['service_day'].dt.month
		self.holo['dow'] = self.holo['service_day'].dt.dayofweek
		self.holo['hour'] = self.holo['tap_datetime'].dt.hour
		self.holo['tod'] = self.holo['hour'].map(tod_mapping)


		## 2. Create HOLO Linked Trips df

		first_tap = self.holo[self.holo.groupby(['linked_UID'])['tap_datetime'].transform(min) == self.holo['tap_datetime']] \
		    .reset_index()[['linked_UID', 'stop_code', 'service_day', 'month', 'dow', 'tod', 'hour']]
		last_tap = self.holo[self.holo.groupby(['linked_UID'])['tap_datetime'].transform(max) == self.holo['tap_datetime']] \
		    .reset_index()[['linked_UID', 'destination_stop_code']]
		self.holo_linked_trips = pd.merge(first_tap, last_tap, on=['linked_UID'])
		
	
	def determine_scheduled_trip_start_time(self):

		# Record the first trip sched_time that is available as the trip_sched_start_time

		sched_start_time = self.apc.groupby(['trip_id', 'trip_date', 'route_id', 'direction']).agg({'sched_time':'min'}).reset_index() \
		    .rename(columns={'sched_time':'trip_sched_start_time'})

		# Add attribute to dataframe
		tmp = self.apc.merge(sched_start_time, on=['trip_id', 'trip_date', 'route_id', 'direction'], how='outer')

		# Attrs of original holo 
		n = self.apc.shape[0]
		n_boardings = self.apc['boardings'].sum()

		# Keep only records with valid trip_sched_start_time
		self.apc = tmp[tmp['trip_sched_start_time'].notna()]
		invalid = tmp[tmp['trip_sched_start_time'].isna()]
		assert (self.apc.shape[0]+invalid.shape[0])==n

		# Convert from datetime to time (e.g., "2021-10-11 04:10:00" to "04:10")
		self.apc['trip_sched_start_time'] = self.apc['trip_sched_start_time'].dt.strftime('%H:%M')

		# Log update
		logger.info("Invalid trip_sched_start_time:")
		logger.info(f"\tRecords:   {invalid.shape[0]:,} / {n:,} ({invalid.shape[0]/n:.2%})")
		logger.info(f"\tBoardings: {invalid['boardings'].sum():,} / {n_boardings:,} ({invalid['boardings'].sum()/n_boardings:.2%})")



	def expansion(self):

		# Determine trip's scheduled start time
		self.determine_scheduled_trip_start_time()

		### Create Trip UID

		# Assign trip_uid
		apc_trip_uid = self.apc[['trip_id', 'route_id', 'direction', 'trip_sched_start_time']].drop_duplicates() \
		    .reset_index(drop=True)
		apc_trip_uid['trip_uid'] = range(apc_trip_uid.shape[0])
		apc_trip_uid.to_csv(os.path.join(self.config['out_dir'], 'expanded_linked_trips.csv'), index=None)
		df1 = self.apc.merge(apc_trip_uid, on=['trip_id', 'route_id', 'direction', 'trip_sched_start_time'], how='left')
		

		### Table 1: APC Trips

		# Keep only relevant columns
		df1 = df1[['trip_uid', 'stop_seq_id', 'stop_code', 'month', 'gtfs_feed', 'dow', 'tod', 'boardings', 'alightings']]


		### Table 2: Trip/Stop-Level Aggregation

		# Aggregate by Trip ID + Stop-level details
		df2 = df1.groupby(['trip_uid', 'stop_code', 'stop_seq_id', 'month', 'gtfs_feed', 'dow', 'tod']) \
		    .agg({'alightings':'count', 'boardings':'sum'}) \
		    .rename(columns={'alightings':'trip_instances'}).reset_index()

		# Calculate average boardings per trip instance
		df2['avg_boardings'] = df2['boardings'] / df2['trip_instances']


		### Table 3: Stop-Level Aggregation

		df3 = df2.groupby(['month', 'gtfs_feed', 'dow', 'tod', 'stop_code']).agg({'avg_boardings':'sum'}).reset_index()


		### Table 4: Stop-Level Aggregation -- Add missing Holo combos

		holo_stops = self.holo[['month', 'gtfs_feed', 'dow', 'tod', 'stop_code']].drop_duplicates()
		df4 = df3.merge(holo_stops, on=['month', 'gtfs_feed', 'dow', 'tod', 'stop_code'], how='outer')


		### Table 5: Stop-Level Aggregation -- Impute missing Holo combos

		# Combinations in need of imputation
		missing_holo = df4[df4['avg_boardings'].isna()].reset_index(drop=True).drop(columns=['avg_boardings'])

		# Run imputation steps
		imputed, missing = self.impute_holo_combos(missing_holo, df3)

		# Combine into one df
		df5 = df4[df4['avg_boardings'].notna()].reset_index(drop=True)
		df5['imputation_level'] = 0
		missing['imputation_level'] = -1
		df5 = pd.concat([df5, imputed, missing])

		# Aggregate away GTFS Feed (needed only to link to stops)
		df5 = df5.groupby(['month', 'dow', 'tod', 'stop_code']).agg({'avg_boardings':'sum'}).reset_index()

		logger.info(f"Unable to impute {missing.shape[0]:,} / {missing_holo.shape[0]:,} Holo combos ({missing.shape[0]/missing_holo.shape[0]:.2%})")


		### Table 6: APC Linked Trips

		# Stop Transfer % (by month, dow, tod given Holo data)
		holo_transfer_pct = self.holo.groupby(['month', 'dow', 'tod', 'stop_code']).agg({'Transfer':'mean'}) \
		    .reset_index().rename(columns={'Transfer':'transfer_pct'})

		# Add transfer % by stop/dow/tod/month
		apc_avg_daily = df5.merge(holo_transfer_pct, on=['month', 'dow', 'tod', 'stop_code'], how='left')

		# APC Average Daily Linked Trips
		apc_avg_daily['apc_linked_trips'] = (1-apc_avg_daily['transfer_pct']) * apc_avg_daily['avg_boardings']

		# For combination where there is APC but no Holo, apc_linked_trips=0
		apc_avg_daily.loc[apc_avg_daily['apc_linked_trips'].isna(), 'apc_linked_trips'] = 0

		# Drop unnecessary cols
		apc_avg_daily.drop(columns=['transfer_pct', 'avg_boardings'], inplace=True)


		### Table 7: HOLO Average Daily Linked Trips

		# Holo TOTAL linked trips by month, dow, tod, stop
		holo_avg_daily = self.holo_linked_trips.groupby(['month', 'dow', 'tod', 'stop_code']).size().to_frame('holo_linked_trips').reset_index()

		# Holo AVG DAILY linked trips by month, dow, tod, stop
		unique_days = self.holo_linked_trips[['month', 'dow', 'service_day']].drop_duplicates() \
			.groupby(['month', 'dow']).size().to_frame('Unique Days').reset_index()
		holo_avg_daily = holo_avg_daily.merge(unique_days, on=['month', 'dow'])
		holo_avg_daily['holo_avg_daily_linked_trips'] = holo_avg_daily['holo_linked_trips'] / holo_avg_daily['Unique Days']

				
		### Table 8: Expansion Factors Table (HOLO + APC)

		# APC + HOLO
		expansion = holo_avg_daily.merge(apc_avg_daily, on=['month', 'dow', 'tod', 'stop_code'], how='left')

		# Expansion Factor
		expansion['expansion_factor'] = expansion['apc_linked_trips'] / expansion['holo_avg_daily_linked_trips']


		### Table 9: Holo Expanded Trips

		self.expanded_linked_trips = self.holo_linked_trips.merge(expansion[['month', 'dow', 'tod', 'stop_code', 'expansion_factor']], on=['month', 'dow', 'tod', 'stop_code'], how='outer')
		assert self.holo_linked_trips.shape[0] == self.expanded_linked_trips.shape[0]


	def impute_holo_combos(self, missing_holo, apc):

		"""
		Impute {'month', 'gtfs_feed', 'dow', 'tod', 'stop_code'} combinations in HOLO but not in APC
		"""

		# Helper dataframe
		imputed = pd.DataFrame()

		# Log update
		missing_holo_n = missing_holo.shape[0]
		logger.info(f"Combinations in APC:                           {self.apc.shape[0]:,}")
		logger.info(f"Combinations in HOLO, not in APC (to impute):  {missing_holo_n:,}")

		## (i.) Round 1: For same month, stop ID, TOD, similar DOW

		rd = 1

		# Get all potential matches
		tmp = missing_holo.merge(apc, on=['month', 'stop_code', 'tod'], how='left').reset_index(drop=True)
		tmp = tmp[tmp['dow_y'].notna()]

		# Keep similar DOWs
		criteria = False
		for dow, similar_dows in self.config['similar_dow'].items():
		    criteria = criteria | (tmp['dow_x']==dow) & (tmp['dow_y'].isin(similar_dows+[dow]))
		tmp = tmp[criteria].drop(columns=['dow_x', 'dow_y'])

		# Calculate Avg Boardings
		tmp = tmp.groupby(['month', 'stop_code', 'tod']).agg({'avg_boardings':'mean'}).reset_index()

		# Update dataframes
		tmp = tmp[tmp['avg_boardings'].notna()]
		tmp['imputation_level'] = rd
		missing_holo = missing_holo.merge(tmp, on=['month', 'stop_code', 'tod'], how='left')
		imputed = pd.concat([imputed, missing_holo[missing_holo['avg_boardings'].notna()]])
		missing_holo = missing_holo[missing_holo['avg_boardings'].isna()].drop(columns=['avg_boardings', 'imputation_level'])

		# Quality check
		assert (missing_holo.shape[0]+imputed.shape[0]) == missing_holo_n

		# logger.info Update
		logger.info(f"Round {rd}")
		logger.info(f"\tRound:       {(imputed['imputation_level']==rd).sum():,} records ({(imputed['imputation_level']==rd).sum()/missing_holo_n:.2%})")
		logger.info(f"\tCumulative:  {imputed.shape[0]:,} records ({imputed.shape[0]/missing_holo_n:.2%})")

		## (ii.) Round 2: For same month, stop ID, DOW

		rd = 2

		# Get all potential matches
		tmp = missing_holo.merge(apc, on=['month', 'stop_code', 'dow'], how='left').reset_index(drop=True)

		# Calculate Avg Boardings
		tmp = tmp.groupby(['month', 'stop_code', 'dow']).agg({'avg_boardings':'mean'}).reset_index()

		# Update dataframes
		tmp = tmp[tmp['avg_boardings'].notna()]
		tmp['imputation_level'] = rd
		missing_holo = missing_holo.merge(tmp, on=['month', 'stop_code', 'dow'], how='left')
		imputed = pd.concat([imputed, missing_holo[missing_holo['avg_boardings'].notna()]])
		missing_holo = missing_holo[missing_holo['avg_boardings'].isna()].drop(columns=['avg_boardings', 'imputation_level'])

		# Quality check
		assert (missing_holo.shape[0]+imputed.shape[0]) == missing_holo_n

		# logger.info Update
		logger.info(f"Round {rd}")
		logger.info(f"\tRound:       {(imputed['imputation_level']==rd).sum():,} records ({(imputed['imputation_level']==rd).sum()/missing_holo_n:.2%})")
		logger.info(f"\tCumulative:  {imputed.shape[0]:,} records ({imputed.shape[0]/missing_holo_n:.2%})")

		## (iii.) Round 3: For same month, same GTFS Feed, DOW, TOD, stops within W (0.5) miles

		rd = 3

		# Get all potential matches
		tmp = missing_holo.merge(apc, on=['gtfs_feed', 'month', 'dow', 'tod'], how='left').reset_index(drop=True)

		# Stops within W miles
		tmp = tmp.merge(self.gtfs_stop_to_stop_dist, on=['gtfs_feed', 'stop_code_x', 'stop_code_y'], how='left')
		logger.info(tmp.columns)
		tmp = tmp[tmp['dist']<0.5]

		# Calculate Avg Boardings
		tmp = tmp.rename(columns={'stop_code_x':'stop_code'}) \
		    .groupby(['dow', 'gtfs_feed', 'month', 'tod', 'stop_code']).agg({'avg_boardings':'mean'}).reset_index()

		# Update dataframes
		tmp = tmp[tmp['avg_boardings'].notna()]
		tmp['imputation_level'] = rd
		missing_holo = missing_holo.merge(tmp, on=['gtfs_feed', 'month', 'stop_code', 'dow', 'tod'], how='left')
		imputed = pd.concat([imputed, missing_holo[missing_holo['avg_boardings'].notna()]])
		missing_holo = missing_holo[missing_holo['avg_boardings'].isna()].drop(columns=['avg_boardings', 'imputation_level'])

		# Quality check
		assert (missing_holo.shape[0]+imputed.shape[0]) == missing_holo_n

		# logger.info Update
		logger.info(f"Round {rd}")
		logger.info(f"\tRound:       {(imputed['imputation_level']==rd).sum():,} records ({(imputed['imputation_level']==rd).sum()/missing_holo_n:.2%})")
		logger.info(f"\tCumulative:  {imputed.shape[0]:,} records ({imputed.shape[0]/missing_holo_n:.2%})")

		## (iv.) Round 4: For same GTFS Feed, DOW, TOD, stops within W (0.5) miles

		rd = 4

		# Get all potential matches
		tmp = missing_holo.merge(apc, on=['gtfs_feed', 'dow', 'tod'], how='left').reset_index(drop=True)

		# Stops within W miles
		tmp = tmp.merge(self.gtfs_stop_to_stop_dist, on=['gtfs_feed', 'stop_code_x', 'stop_code_y'], how='left')
		tmp = tmp[tmp['dist']<0.5]

		# Calculate Avg Boardings
		tmp = tmp.rename(columns={'stop_code_x':'stop_code'}) \
		    .groupby(['dow', 'gtfs_feed', 'tod', 'stop_code']).agg({'avg_boardings':'mean'}).reset_index()

		# Update dataframes
		tmp = tmp[tmp['avg_boardings'].notna()]
		tmp['imputation_level'] = rd
		missing_holo = missing_holo.merge(tmp, on=['gtfs_feed', 'stop_code', 'dow', 'tod'], how='left')
		imputed = pd.concat([imputed, missing_holo[missing_holo['avg_boardings'].notna()]])
		missing_holo = missing_holo[missing_holo['avg_boardings'].isna()].drop(columns=['avg_boardings', 'imputation_level'])

		# Quality check
		assert (missing_holo.shape[0]+imputed.shape[0]) == missing_holo_n

		# logger.info Update
		logger.info(f"Round {rd}")
		logger.info(f"\tRound:       {(imputed['imputation_level']==rd).sum():,} records ({(imputed['imputation_level']==rd).sum()/missing_holo_n:.2%})")
		logger.info(f"\tCumulative:  {imputed.shape[0]:,} records ({imputed.shape[0]/missing_holo_n:.2%})")

		## (v.) Round 5: Same dow, same tod, same stop_code, within 1 month

		rd = 5

		tmp = missing_holo.merge(apc, on=['dow', 'tod', 'stop_code'], how='inner').reset_index(drop=True)

		# Within 1 months
		tmp = tmp[(tmp['month_x']-tmp['month_y']).abs() <= 1]

		# Calculate Avg Boardings
		tmp = tmp.groupby(['dow', 'tod', 'stop_code']).agg({'avg_boardings':'mean'}).reset_index()

		# Update dataframes
		tmp = tmp[tmp['avg_boardings'].notna()]
		tmp['imputation_level'] = rd
		missing_holo = missing_holo.merge(tmp, on=['stop_code', 'tod', 'dow'], how='left')
		imputed = pd.concat([imputed, missing_holo[missing_holo['avg_boardings'].notna()]])
		missing_holo = missing_holo[missing_holo['avg_boardings'].isna()].drop(columns=['avg_boardings', 'imputation_level'])

		# Quality check
		assert (missing_holo.shape[0]+imputed.shape[0]) == missing_holo_n

		# logger.info Update
		logger.info(f"Round {rd}")
		logger.info(f"\tRound:       {(imputed['imputation_level']==rd).sum():,} records ({(imputed['imputation_level']==rd).sum()/missing_holo_n:.2%})")
		logger.info(f"\tCumulative:  {imputed.shape[0]:,} records ({imputed.shape[0]/missing_holo_n:.2%})")

		## (vi.) Round 6: Same dow, same tod, same stop_code, within 1 month

		rd = 6

		tmp = missing_holo.merge(apc, on=['dow', 'tod', 'stop_code'], how='inner').reset_index(drop=True)

		# Within 1 months
		tmp = tmp[(tmp['month_x']-tmp['month_y']).abs() <= 2]

		# Calculate Avg Boardings
		tmp = tmp.groupby(['dow', 'tod', 'stop_code']).agg({'avg_boardings':'mean'}).reset_index()

		# Update dataframes
		tmp = tmp[tmp['avg_boardings'].notna()]
		tmp['imputation_level'] = rd
		missing_holo = missing_holo.merge(tmp, on=['stop_code', 'tod', 'dow'], how='left')
		imputed = pd.concat([imputed, missing_holo[missing_holo['avg_boardings'].notna()]])
		missing_holo = missing_holo[missing_holo['avg_boardings'].isna()].drop(columns=['avg_boardings', 'imputation_level'])

		# Quality check
		assert (missing_holo.shape[0]+imputed.shape[0]) == missing_holo_n

		# logger.info Update
		logger.info(f"Round {rd}")
		logger.info(f"\tRound:       {(imputed['imputation_level']==rd).sum():,} records ({(imputed['imputation_level']==rd).sum()/missing_holo_n:.2%})")
		logger.info(f"\tCumulative:  {imputed.shape[0]:,} records ({imputed.shape[0]/missing_holo_n:.2%})")

		return imputed, missing_holo


	def write_table(self):
		logger.info('Writing Holo Expanded Trips table...')
		if self.expanded_linked_trips is None:
			logger.info("Run expansion() function before trying to write out expanded linked trips.")
		else:
			self.expanded_linked_trips.to_csv(os.path.join(self.config['out_dir'], 'expanded_linked_trips.csv'), index=None)


def main():

	# Generate Linked Trips
	expansionModule = ExpansionModule()
	expansionModule.preprocess()
	expansionModule.expansion()
	expansionModule.write_table()


if __name__ == "__main__":
	main()