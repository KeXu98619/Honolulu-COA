import yaml
import pandas as pd
import sys
import numpy as np
import os
import time

# Set path to enable module imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# Set up logger
import utility_module.logger as logging 
logger = logging.get_logger('__name__','../../log_files/preprocessing.log')


class ProcessHoloData:
	"""
	Trip chaining & destination inference for HOLO Card data. 
	Methods: 
		- __init__()			   : Read in processed config file, HOLO Data
		- identify_tranfers()	   : Identify if the tap is a transfer. Add Boolean attribute indicating Transfer. 
		- identify_linked_trips()  : Identify linked trips. Add linked_trip_uid attribute. 
	"""

	def __init__(self):

		## Config File
		try:
			with open(os.path.join("config_files", "config_linked_trips.yaml"), "r") as f:
				self.config = yaml.safe_load(f)
		except:
			logger.exception("Could not read config file")

		## Processed Holo Data
		try:
			logger.info("Reading Processed HOLO Card data...")
			self.df = pd.read_csv(os.path.join(self.config['out_dir'], 'holo_processed.csv'), parse_dates=['tap_datetime', 'service_day'])
		except: 
			logger.exception("Could not read HOLO Data.")

		# GTFS Stop distance mapping
		try:
			logger.info("Reading GTFS data...")
			self.gtfs_nearest_stop = pd.read_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_nearest_stop.csv'))
			self.gtfs_stop_to_stop_dist = pd.read_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_stop_to_stop_dist.csv'))
		except: 
			logger.exception("Could not read GTFS Data.")

		# Keep only valid Holo trips
		self.df = self.df[self.df['valid']==1].reset_index(drop=True)


	def identify_transfers(self, df):
		"""
		Create Boolean `Transfer` attribute, indicating if record is a transfer or not. 

		Transfer characteristics:
			- Tap on same service day as previous tap
			- Tap within x (90) mins of previous tap
			- Tap has different Route ID than previous tap. 
		"""

		def transfer_rds(df):
	
			## (0) Preprocessing

			# Label first tap of card
			df = df.merge(
				df.groupby(['holocard_uid']).agg({'tap_datetime':'min'}).reset_index().rename(columns={'tap_datetime':'first_tap'}), 
				on=['holocard_uid'], how='left')

			# Sort by card number, tap datetime
			df = df.sort_values(['holocard_uid', 'tap_datetime']).reset_index(drop=True)

			# Previous Tap attributes
			df['previous_tap_datetime'] = df['tap_datetime'].shift()
			df['mins_since_last_tap'] = (df['tap_datetime'] - df['previous_tap_datetime']).dt.total_seconds() / 60
			df['previous_tap_service_day'] = df['service_day'].shift()
			df['previous_tap_route'] = df['route_id'].shift()
			df['previous_tap_dest_stop'] = df['destination_stop_code'].shift()

			# Fill attributes of first tap record with Nulls
			df.loc[df['first_tap']==df['tap_datetime'], 'mins_since_last_tap'] = np.nan
			df.loc[df['first_tap']==df['tap_datetime'], 'previous_tap_route'] = np.nan
			df.loc[df['first_tap']==df['tap_datetime'], 'previous_tap_service_day'] = np.nan
			df.loc[df['first_tap']==df['tap_datetime'], 'destination_stop_code'] = np.nan

			# Get distance from previous tap's alighting location to this tap's start location
			df = df.merge(self.gtfs_stop_to_stop_dist, 
				 left_on=['gtfs_feed', 'stop_code', 'previous_tap_dest_stop'], 
				 right_on=['gtfs_feed', 'stop_code_x', 'stop_code_y'], 
				 how='left').drop(columns=['stop_code_x', 'stop_code_y'])
			
			## (1) Identify Transfers
			df['Transfer'] = False
			criteria = (df['service_day'] == df['previous_tap_service_day']) & \
				(df['route_id'] != df['previous_tap_route']) & \
				(df['mins_since_last_tap'] <= self.config['transfer_time_threshold']) & \
				(df['dist'] <= self.config['dist_from_previous_stop'])
			df.loc[criteria, 'Transfer'] = True
			
			## (2) For a tap to be a transfer, it has to be within x mins of the *first tap of the trip*

			# Sort by card number, tap datetime
			df = df.sort_values(['holocard_uid', 'tap_datetime']).reset_index(drop=True)

			# Create temporary Linked Trip UID
			df.loc[~df['Transfer'], 'linked_UID'] = list(range((~df['Transfer']).sum()))
			df['linked_UID'] = df['linked_UID'].ffill(axis=0).astype(int)

			# Calculate mins since first tap
			df = df.merge(df.groupby(['linked_UID']).agg({'tap_datetime':'min'}).reset_index().rename(columns={'tap_datetime':'linked_trip_start_time'}), on=['linked_UID'], how='left')
			df['mins_since_linked_start'] = (df['tap_datetime'] - df['linked_trip_start_time']).dt.total_seconds() / 60
			
			# Keep only rows with time within threshold
			df.loc[df['mins_since_linked_start'] > self.config['transfer_time_threshold'], 'Transfer'] = np.nan

			# Drop unnecessary columns
			df.drop(columns=['mins_since_linked_start', 'linked_trip_start_time', 'linked_UID', 'first_tap', 'previous_tap_datetime', 'previous_tap_service_day', 'previous_tap_route', 'dist'], inplace=True)
			
			return df

		logger.info("Identifying Transfers...")

		# Taps with more than one tap on that service day
		n = df.shape[0]

		# Initialize Variables
		rd = 1
		out = pd.DataFrame()

		while df.shape[0] > 0:
			
			print(f"Round {rd}")
			
			# Run Transfer Inference algorithm
			df = transfer_rds(df)
			
			# Add inferred results to main df
			out = pd.concat([out, df[df['Transfer'].notna()].reset_index(drop=True)])
			
			# Get df of rows still to infer
			df = df[df['Transfer'].isna()].reset_index(drop=True)
			
			# Print update
			print(f'\tStill to infer: {df.shape[0]:,}')
			
			# QA/QC
			assert (out.shape[0] + df.shape[0]) == n
			
			rd+=1

		return out
		

	def assign_linked_trips(self, df):

		"""
		1. Create linked trip ID. 
		2. Disallow repeat routes on linked trip. 
		3. Re-assign linked trip ID
		"""
		
		## 1. Create linked trip ID
		def assign_ID(df):

			# Sort by card number, tap datetime
			df = df.sort_values(['holocard_uid', 'tap_datetime']).reset_index(drop=True)

			# Create Linked Trip UID
			df.loc[df['Transfer']==False, 'linked_UID'] = list(range((df['Transfer']==False).sum()))
			df['linked_UID'] = df['linked_UID'].ffill(axis=0).astype(int)

			return df
		df = assign_ID(df)

		print(f"(1) UNIQUE TRIPS: {df['linked_UID'].nunique()}") # TMP
		
		## 2. Disallow repeat routes on linked trip
		
		# 2.1 Split into "acceptable" linked trips and "problem" linked trips
		tmp = df.groupby(['linked_UID']).agg({'route_id':'nunique', 'transaction_uid':'count'}).reset_index() \
			.rename(columns = {'transaction_uid':'Unlinked Trips', 'route_id':'Unique Routes'})
		problem_linked_UIDs = tmp[tmp['Unlinked Trips'] != tmp['Unique Routes']]['linked_UID']
		problem_trips = df[df['linked_UID'].isin(problem_linked_UIDs)]
		acceptable_trips = df[~df['linked_UID'].isin(problem_linked_UIDs)].drop(columns=['linked_UID'])
		
		# 2.2 Re-assign Transfers
		def reassign_transfer(df):
			routes_traversed = []
			for idx, tap in df.iterrows():
				if tap['route_id'] in routes_traversed:
					df.loc[idx, 'Transfer'] = False
					routes_traversed = []
				routes_traversed.append(tap['route_id'])
			return df
		problem_trips = problem_trips.groupby(['linked_UID']).apply(reassign_transfer).drop(columns=['linked_UID'])
		
		# 3. Re-assign linked trip ID
		df = assign_ID(pd.concat([acceptable_trips, problem_trips]))

		print(f"(2) UNIQUE TRIPS: {df['linked_UID'].nunique()}") # TMP
		
		# 4. QA/QC
		tmp = df.groupby(['linked_UID']).agg({'route_id':'nunique', 'transaction_uid':'count'}).reset_index() \
			.rename(columns = {'transaction_uid':'Unlinked Trips', 'route_id':'Unique Routes'})
		problem_linked_UIDs = tmp[tmp['Unlinked Trips'] != tmp['Unique Routes']]['linked_UID']
		problem_trips = df[df['linked_UID'].isin(problem_linked_UIDs)]
		assert len(problem_trips)==0
		
		return df


	def infer_destinations(self):
		
		start_time = time.time()

		### ROUND 0

		# Keep only relevant columns
		self.df = self.df[['transaction_uid', 'holocard_uid', 'tap_datetime', 'service_day', 'gtfs_feed', 'stop_code', 'route_id']]

		# Original number of records
		n = self.df.shape[0]

		### Round 0 -- Initialization of destinations
		rd = 0

		# Sort by card number, tap datetime
		self.df = self.df.sort_values(['holocard_uid', 'tap_datetime'])

		# Datetime helpers
		self.df['hour'] = self.df['tap_datetime'].dt.hour
		self.df['dow'] = self.df['service_day'].dt.dayofweek

		# Number of transactions that service day
		self.df = self.df.merge(
			self.df.groupby(['holocard_uid', 'service_day']).size().to_frame('transactions').reset_index(),
			on=['holocard_uid', 'service_day'], how='left')

		# Split into transactions we want to deal with in round 0 vs rounds 1+
		to_infer = self.df[self.df['transactions']==1].reset_index(drop=True).drop(columns=['transactions'])
		self.df = self.df[self.df['transactions']>1].reset_index(drop=True).drop(columns=['transactions'])

		## Next Tap attributes
		self.df['next_tap_stop_code'] = self.df['stop_code'].shift(-1)

		## Fix case of last tap of service day -- set next_tap attributes to be *first* tap of service day

		# Add stop_id of first tap of service day
		self.df = self.df.merge(
			self.df[self.df.groupby(['holocard_uid', 'service_day'])['tap_datetime'].transform(min) == self.df['tap_datetime']][['holocard_uid', 'service_day', 'stop_code']].rename(columns={'stop_code':'service_day_first_tap'}),
			how='left', on=['holocard_uid', 'service_day'])

		# Determine last tap of day
		tmp = self.df[self.df.groupby(['holocard_uid', 'service_day'])['tap_datetime'].transform(max) == self.df['tap_datetime']][['transaction_uid']]
		tmp['last_service_day_tap'] = True
		self.df = self.df.merge(tmp, on=['transaction_uid'], how='left')
		self.df['last_service_day_tap'].fillna(False, inplace=True)
		# Assign next tap stop for last tap of day to stop_code of first tap of day
		self.df.loc[self.df['last_service_day_tap'], 'next_tap_stop_code'] = self.df.loc[self.df['last_service_day_tap'], 'service_day_first_tap']

		## Clean up 
		self.df.drop(columns=['service_day_first_tap', 'last_service_day_tap'], inplace=True)
		self.df['next_tap_stop_code'] = self.df['next_tap_stop_code'].astype(int)

		## INFER DESTINATION
		inferred = self.df.merge(
			self.gtfs_nearest_stop.rename(columns={'stop_code':'next_tap_stop_code', 'nearest_stop':'destination_stop_code'}) \
				.drop(columns=['dist']), 
			on=['gtfs_feed', 'route_id', 'next_tap_stop_code'], how='left').reset_index(drop=True)

		# Clean up
		inferred.drop(columns=['next_tap_stop_code'], inplace=True)
		inferred['inference_level'] = rd

		## IDENTIFY TRANSFERS
		inferred = self.identify_transfers(inferred)
		to_infer['Transfer'] = False

		# Create Linked UID
		self.df = self.assign_linked_trips(pd.concat([inferred, to_infer]))
		inferred = self.df[self.df['destination_stop_code'].notna()].reset_index(drop=True)
		to_infer = self.df[self.df['destination_stop_code'].isna()].reset_index(drop=True).drop(columns=['destination_stop_code'])
		
		# QA/QC
		assert (inferred.shape[0]+to_infer.shape[0])==n

		# logger.info Update
		logger.info(f"\tInferred destination for {(inferred['inference_level']==rd).sum():,} records ({(inferred['inference_level']==rd).sum()/n:.2%})")
		logger.info(f"\tRuntime {round((time.time()-start_time)/60, 2)} mins")


		## 2. Unlinked -> Linked Trips 

		# Use linked trips whose destinations were inferred for inference base. 
		first_stage = inferred[inferred.groupby(['linked_UID'])['tap_datetime'].transform(min) == inferred['tap_datetime']] \
			[['linked_UID', 'tap_datetime', 'stop_code', 'hour', 'dow']]
		last_stage = inferred[inferred.groupby(['linked_UID'])['tap_datetime'].transform(max) == inferred['tap_datetime']] \
			[['linked_UID', 'destination_stop_code']]
		linked_trips = inferred[['linked_UID', 'holocard_uid', 'service_day', 'gtfs_feed']].drop_duplicates() \
			.merge(first_stage, on=['linked_UID'], how='left') \
			.merge(last_stage, on=['linked_UID'], how='left') \
			.reset_index(drop=True)

		# Ensure all linked trips are accounted for
		assert linked_trips.shape[0] == inferred['linked_UID'].nunique()


		## 3. Inference Rounds

		for rd, params in self.config['destination_inference_rounds'].items():

			logger.info(f'Inference round {rd}')
			start_time = time.time()

			# Merge inferred & To Infer dfs based on "same" attribute cols. 
			tmp = to_infer[['transaction_uid', 'tap_datetime', 'holocard_uid', 'service_day', 'Transfer', 'gtfs_feed', 'stop_code', 'hour', 'dow']].merge(
				linked_trips, on=[k for k,v in params.items() if v=='Same'], how='left')
			
			# Drop rows with no potential match
			tmp = tmp[tmp['destination_stop_code'].notna()]
			
			# Cast as int
			tmp['destination_stop_code'] = tmp['destination_stop_code'].astype(int)

			# Keep only potential matches within Z months
			tmp = tmp[(tmp['tap_datetime_x'] - tmp['tap_datetime_y']).dt.days.abs() < params['timeframe']]
			
			## Hour
			if (type(params['hour'])==int):
				tmp = tmp[(tmp['hour_x'] - tmp['hour_y']).abs() <= params['hour']]

			## Day of Week
			if params['dow']=='Similar Days':
				criteria = False
				for dow, similar_dows in self.config['similar_dow'].items():
					criteria = criteria | (tmp['dow_x']==dow) & (tmp['dow_y'].isin(similar_dows+[dow]))
				tmp = tmp[criteria]
				
			## Stops within X miles
			if type(params['stop_code'])==float:
				break
			
			# For each transaction_uid, get list of possible stop_ids
			tmp = tmp.groupby(['transaction_uid'])['destination_stop_code'].apply(list) \
				.reset_index(name='stop_ids')
			
			## Assign stop IDs

			# For records with only one possible stop ID, assign that. 
			criteria = tmp['stop_ids'].str.len() == 1
			tmp.loc[criteria, 'destination_stop_code'] = tmp.loc[criteria, 'stop_ids'].str[0]

			# Else, sample from list of potential stop IDs
			criteria = tmp['stop_ids'].str.len() > 1
			tmp.loc[criteria, 'destination_stop_code'] = tmp.loc[criteria, 'stop_ids'].apply(lambda x : np.random.choice(x))
			
			# Add back & update record attributes
			tmp = tmp[['transaction_uid', 'destination_stop_code']].merge(to_infer, on='transaction_uid', how='left')
			tmp['destination_stop_code'] = tmp['destination_stop_code'].astype(int)
			tmp['inference_level'] = rd

			# Update DFs
			to_infer = to_infer[~to_infer['transaction_uid'].isin(tmp['transaction_uid'])].reset_index(drop=True)
			inferred = pd.concat([inferred, tmp]).reset_index(drop=True)

			# QA/QC
			assert (inferred.shape[0]+to_infer.shape[0])==n

			# Print Update
			logger.info(f"\tInferred destination for {(inferred['inference_level']==rd).sum():,} records ({(inferred['inference_level']==rd).sum()/n:.2%})")
			logger.info(f"\tRuntime {round((time.time()-start_time)/60, 2)} mins")

			self.df = pd.concat([inferred, to_infer])

		## 4. Non-Inferred destinations

		self.df.loc[self.df['destination_stop_code'].isna(), 'inference_level'] = -1
		self.df.loc[self.df['destination_stop_code'].isna(), 'destination_stop_code'] = -999
		self.df['inference_level'] = self.df['inference_level'].astype(int)
		self.df['destination_stop_code'] = self.df['destination_stop_code'].astype(int)

	def write_tables(self):

		logger.info("Writing out...")

		self.df.to_csv(os.path.join(self.config['out_dir'], "holo_linked_trips.csv"), index=None)


def main():

	# Generate Linked Trips
	processHolo = ProcessHoloData()
	processHolo.infer_destinations()
	processHolo.write_tables()


if __name__ == "__main__":
	main()