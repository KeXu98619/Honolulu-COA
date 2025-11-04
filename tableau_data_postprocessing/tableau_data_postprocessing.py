import pandas as pd
import os
import geopandas as gpd
import pantab
import yaml
import sys

# Set path to enable module imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# Set up logger
import utility_module.logger as logging 
logger = logging.get_logger('__name__','../../log_files/tableau_data_postprocessing.log')


class TableauDataPostprocessing:
	"""
	Process data for use in Tableau.
	Methods: 
		- __init__()	   : Read in processed config file, data
		- tableau_data()   : Process all tableau data
	"""

	def __init__(self):

		## Config
		try:
			with open(os.path.join("config_files", "config_Tableau_post_processing.yaml"), "r") as f:
				self.config = yaml.safe_load(f)
		except:
			logger.exception("Could not read config file")



	def neighborhood_boards(self):

		logger.info("Processing Neighborhood Boards...")

		# Read in & clean up
		boards = gpd.read_file(os.path.join(self.config['data_dir'], 'input', 'geo', 'Neighborhood_Boards.shp')) \
		    [['BOARD_NUM', 'geometry']].rename(columns={'BOARD_NUM':'Board'}).set_crs(epsg=4326).to_crs(epsg=3763)
		boards['Board'] = boards['Board'].astype(int)
		assert boards.crs.axis_info[0].unit_name == 'metre'

		# Add Board Name
		board_names = pd.read_csv(os.path.join(self.config['data_dir'], 'input', 'geo', 'board_labels.csv'))
		boards = boards.merge(board_names, on=['Board'], how='left').rename(columns={'Label':'Board Name'})

		# Deal with encoding where destination is not inferred
		board_NA = pd.DataFrame({'Board':[-1], 'Board Name':'Not Inferred'})
		boards = pd.concat([boards, board_NA])

		# Write out
		boards.to_file(os.path.join(self.config['data_dir'], 'output', 'Tableau', 'geo', 'boards.shp'), index=None)



	def districts(self):

		logger.info("Processing Districts...")

		# Read in data
		districts = gpd.read_file(os.path.join(self.config['data_dir'], 'input', 'geo', 'DPA.shp')) \
		    [['DPA', 'geometry']].rename(columns={'DPA':'Dist Name'}).set_crs(epsg=4326).to_crs(epsg=3763)

		# Add District index
		districts['District'] = range(districts.shape[0])

		# Deal with encoding where destination is not inferred
		district_NA = pd.DataFrame({'District':[-1], 'Dist Name':'Not Inferred'})
		districts = pd.concat([districts, district_NA]).reset_index(drop=True)

		# Write out
		districts.to_file(os.path.join(self.config['data_dir'], 'output', 'Tableau', 'geo', 'districts.shp'), index=None)


	def stops(self):

		logger.info("Processing Stops...")

		# Read in GTFS Stops file
		stops = pd.read_csv(os.path.join(self.config['data_dir'], 'output', 'gtfs', 'gtfs_stops.csv'))

		# Flatten GTFS dimension
		stops = stops.groupby(['stop_code']).last().reset_index()

		# Create shapefile
		stops = gpd.GeoDataFrame(stops, geometry=gpd.points_from_xy(stops['stop_lon'], stops['stop_lat']), crs='epsg:4326') \
		    [['stop_code', 'stop_name', 'geometry']].to_crs(epsg=3763)
		assert stops.crs.axis_info[0].unit_name == 'metre'

		# Reformat name
		stops['stop_name'] = stops['stop_name'].str.title()

		## Districts

		# Read in file
		districts = gpd.read_file(os.path.join(self.config['data_dir'], 'output', 'Tableau', 'geo', 'districts.shp'))

		# Stop <> District mapping
		district_alias = dict(zip(districts['Dist Name'], districts['District']))
		stop_district_map = stops.sjoin(districts, how="left", predicate='intersects') \
		    [['stop_code', 'District']]

		# Fill NA by hand (slightly outside the range of the district -- see in QGIS)
		na_stop_mapping = {
		    2555 : district_alias['NORTH SHORE'],
		    2310 : district_alias['NORTH SHORE'],
		    2399 : district_alias['KOOLAULOA'],
		    2474 : district_alias['KOOLAULOA'], 
		    2473 : district_alias['KOOLAULOA'],
		    1580 : district_alias['KOOLAUPOKO'], 
		    988 : district_alias['PUC'],
		    3342 : district_alias['PUC']
		}
		stop_district_map.loc[stop_district_map['District'].isna(), 'District'] = \
		    stop_district_map.loc[stop_district_map['District'].isna(), 'stop_code'].apply(lambda x : na_stop_mapping[x])

		# Add to stops
		stops = stops.merge(stop_district_map, on=['stop_code'], how='left')
		assert stops['District'].isna().sum()==0
		stops['District'] = stops['District'].astype(int)
		stop_district_map['District'] = stop_district_map['District'].astype(int)

		## Nieghborhood Boards

		# Read in file
		boards = gpd.read_file(os.path.join(self.config['data_dir'], 'output', 'Tableau', 'geo', 'boards.shp'))

		# Stop <> Board mapping
		stop_board_map = stops.sjoin(boards, how="left", predicate='intersects') \
		    [['stop_code', 'Board']]

		# Fill NA by hand (slightly outside the range of the board -- see in QGIS)
		na_stop_mapping = {
		    2464 : 28,
		    2468 : 28,
		    2471 : 28,
		    2493 : 28
		}
		stop_board_map.loc[stop_board_map['Board'].isna(), 'Board'] = \
		    stop_board_map.loc[stop_board_map['Board'].isna(), 'stop_code'].apply(lambda x : na_stop_mapping[x])

		# Add to stops
		stops = stops.merge(stop_board_map, on=['stop_code'], how='left')
		assert stops['Board'].isna().sum()==0
		stops['Board'] = stops['Board'].astype(int)

		# Deal with encoding where destination is not inferred
		stop_NA = pd.DataFrame({'stop_code':[-999], 'stop_name':'Not Inferred'})
		stops = pd.concat([stops, stop_NA])

		# Write out
		stops.to_file(os.path.join(self.config['data_dir'], 'output', 'Tableau', 'geo', 'stops.shp'), index=None)



	def routes(self):

		logger.info("Processing Routes...")

		# Read in GTFS Routes
		route_names = pd.read_csv(os.path.join(self.config['data_dir'], 'output', 'gtfs', 'gtfs_routes.csv')) \
		    [['gtfs_feed', 'route_id', 'route_long_name']]

		# For routes with different route names across GTFS feeds, use the most recent feed as the route name
		route_names = route_names[route_names.groupby(['route_id'])['gtfs_feed'].transform(max)==route_names['gtfs_feed']] \
		    .reset_index(drop=True)[['route_id', 'route_long_name']].rename(columns={'route_long_name':'route_name'})

		# Ensure each route has exactly one name
		assert route_names['route_id'].value_counts().max()==1

		# Write out
		route_names.to_csv(os.path.join(self.config['data_dir'], 'output', 'Tableau', 'route_names.csv'), index=None)

	
	def geo_data(self):
		"""
		Wrapper for geo file creation functions.
		"""
		self.neighborhood_boards()
		self.districts()
		self.stops()
		self.routes()



	def transactions(self):

		logger.info("Processing Transactions...")

		## 1) Keep only relevant Service Days

		# Expanded linked trips (to get service days to use)
		expanded_linked_trips = pd.read_csv(os.path.join(self.config['data_dir'], 'output', 'expanded_linked_trips.csv'), parse_dates=['service_day'])

		# Linked Trips
		transactions = pd.read_csv(os.path.join(self.config['data_dir'], 'output', 'holo_linked_trips.csv'), parse_dates=['service_day', 'tap_datetime'])

		# Keep only service days within expanded linked trips (i.e., those in HOLO & APC)
		transactions = transactions[transactions['service_day'].isin(expanded_linked_trips['service_day'])] \
		    .reset_index(drop=True)

		
		## 2) Add attributes from original Transactions dataset

		# Transactions - 1st round of processing
		transactions_raw = pd.read_csv(os.path.join(self.config['data_dir'], 'output', 'holo_processed.csv'))

		# Columns to take
		cols = ['transaction_uid', 'fare_category_id', 'product_id']

		# Add attributes
		transactions = transactions.merge(transactions_raw[cols], on='transaction_uid', how='left')


		## 3) Helper Attributes

		# Number of Transfers in linked trip
		tmp = transactions.groupby(['linked_UID']).size().to_frame('Transactions in Linked Trip').reset_index()
		transactions = transactions.merge(tmp, on=['linked_UID'])

		# Time of Day
		tod_mapping = {}
		for name, hours in self.config['tod_def'].items():
		    for hour in hours: 
		        tod_mapping[hour] = name

		# Helper columns
		transactions['month'] = transactions['service_day'].dt.month
		transactions['dow'] = transactions['service_day'].dt.day_name()
		transactions['hour'] = transactions['tap_datetime'].dt.hour
		transactions['tod'] = transactions['hour'].map(tod_mapping)


		## 4) Alias Variables

		## Rename attributes
		transactions.rename(columns={
		    'stop_code' : 'stop_code - Board', 
		    'destination_stop_code' : 'stop_code - Alight',
		    'linked_UID' : 'linked_uid'
		}, inplace=True)

		## Keep only relevant attributes
		cols = ['transaction_uid', 'holocard_uid', 'Transfer', 'linked_uid', 
		        'month', 'dow', 'tod', 'hour', 'tap_datetime', 'gtfs_feed', 'service_day', 
		        'stop_code - Board', 'stop_code - Alight', 'route_id',
		        'product_id', 'fare_category_id', 'Transactions in Linked Trip']
		transactions = transactions[cols]

		## Alias
		transactions['dow'] = transactions['dow'].map({'Monday':0, 'Tuesday':1, 'Wednesday':2, 'Thursday':3, 'Friday':4, 'Saturday':5, 'Sunday':6}).astype(int)
		transactions['tod'] = transactions['tod'].map({'Early AM':0, 'AM Peak':1, 'Midday':2, 'PM Peak':3, 'Late Night':4, 'Owl':5}).astype(int)
		transactions['Transfer'] = transactions['Transfer'].astype(int)


		## 5) Write Out

		pantab.frame_to_hyper(transactions, os.path.join(self.config['data_dir'], 'output', 'Tableau', 'transactions_Tableau.hyper'), table="Transactions")



	def user_segments(self):

		logger.info("Processing User Segments...")

		## 1) Set up

		# Read in transactions data
		transactions = pantab.frame_from_hyper(os.path.join(self.config['data_dir'], 'output', 'Tableau', 'transactions_Tableau.hyper'), table="Transactions")

		# Initialize df
		user_segmentations = pd.DataFrame()


		## 2) DOW
		transactions['Weekday'] = 1
		transactions.loc[transactions['dow'].isin([5, 6]), 'Weekday'] = 0
		tmp = transactions.groupby(['holocard_uid', 'month']).agg({'Weekday':'mean'}).reset_index()
		tmp['DoW'] = tmp['Weekday']>self.config['weekday_user_thresh']
		tmp.loc[tmp['DoW'], 'Segmentation - DoW'] = 'Weekday User'
		tmp.loc[~tmp['DoW'], 'Segmentation - DoW'] = 'All-Week User'
		user_segmentations = tmp[['holocard_uid', 'month', 'Segmentation - DoW']].reset_index(drop=True)
		transactions.drop(columns=['Weekday'], inplace=True)
		user_segmentations['Segmentation - DoW'] = user_segmentations['Segmentation - DoW'].map({'Weekday User':1, 'All-Week User':2})


		## 3) TOD
		transactions['Peak'] = False
		transactions.loc[transactions['tod'].isin([1,3]), 'Peak'] = True
		tmp = transactions.groupby(['holocard_uid', 'month']).agg({'Peak':'mean'}).reset_index()
		tmp['Segmentation - ToD'] = 'All Day User'
		tmp.loc[tmp['Peak']<=(1-self.config['tod_user_thresh']), 'Segmentation - ToD'] = 'Off-Peak User'
		tmp.loc[tmp['Peak']>self.config['tod_user_thresh'], 'Segmentation - ToD'] = 'Peak User'
		user_segmentations = user_segmentations.merge(tmp[['holocard_uid', 'month', 'Segmentation - ToD']], on=['holocard_uid', 'month'], how='outer')
		transactions.drop(columns=['Peak'], inplace=True)
		user_segmentations['Segmentation - ToD'] = user_segmentations['Segmentation - ToD'].map({'Peak User':1, 'Off-Peak User':2, 'All Day User':3})


		## 4) Product Type
		tmp = transactions.groupby(['holocard_uid', 'month'])['product_id'].agg(pd.Series.mode).reset_index() \
		    .rename(columns={'product_id':'Segmentation - Product Type'})
		user_segmentations = user_segmentations.merge(tmp, on=['holocard_uid', 'month'], how='outer')

		## Daily Usage -- Average Linked Trips per week
		tmp = transactions.groupby(['holocard_uid', 'month']).agg({'linked_uid':'nunique'}).reset_index() \
		    .rename(columns={'linked_uid':'Linked Trips'})

		days_all = transactions[['month', 'service_day']].drop_duplicates() \
		    .groupby(['month']).size().to_frame().reset_index() \
		        .rename(columns={0:'Total Unique Days'})
		days_all['Weeks'] = days_all['Total Unique Days']/7
		tmp = tmp.merge(days_all, on=['month'])
		tmp['linked_trips_per_week'] = (tmp['Linked Trips']/tmp['Weeks'])
		bins = self.config['usage_bins']
		labels = bins[:-1]
		tmp['Segmentation - Weekly Usage'] = pd.cut(list(tmp['linked_trips_per_week']), bins=bins, labels=labels)
		user_segmentations = user_segmentations.merge(tmp[['holocard_uid', 'month', 'Segmentation - Weekly Usage']], on=['holocard_uid', 'month'], how='outer')


		## 5) QA/QC
		assert user_segmentations.isna().sum().sum()==0


		## 6) Write Out
		user_segmentations.to_csv(os.path.join(self.config['data_dir'], 'output', 'Tableau', 'user_segmentations.csv'), index=None)



	def expanded_linked_trips(self):

		logger.info("Processing Expanded Linked Trips...")

		## 1) Set up 

		# Read in data
		expanded_linked_trips = pd.read_csv(os.path.join(self.config['data_dir'], 'output', 'expanded_linked_trips.csv'), parse_dates=['service_day'])
		transactions = pantab.frame_from_hyper(os.path.join(self.config['data_dir'], 'output', 'Tableau', 'transactions_Tableau.hyper'), table="Transactions")

		# Original total expanded linked trips (for QA)
		n = expanded_linked_trips['expansion_factor'].sum()


		## 2) Alias variables

		## Rename attributes
		expanded_linked_trips.rename(columns={
		    'stop_code' : 'stop_code - Origin', 
		    'destination_stop_code' : 'stop_code - Destination',
		    'linked_UID' : 'linked_uid', 
		    'expansion_factor' : 'weight'
		}, inplace=True)

		# Keep only relevant columns
		cols = ['linked_uid', 'service_day', 'month', 'dow', 'tod', 'stop_code - Origin', 'stop_code - Destination', 'weight']
		expanded_linked_trips = expanded_linked_trips[cols]

		## Alias
		expanded_linked_trips['tod'] = expanded_linked_trips['tod'].map({'Early AM':0, 'AM Peak':1, 'Midday':2, 'PM Peak':3, 'Late Night':4, 'Owl':5}).astype(int)


		## 3) Add Attributes from Transactions

		# 3.1) Timestamp of first tap of linked trip
		tmp = transactions[transactions.groupby(['linked_uid'])['tap_datetime'].transform(min) == transactions['tap_datetime']] \
		    [['linked_uid', 'tap_datetime']]
		expanded_linked_trips = expanded_linked_trips.merge(tmp, on='linked_uid', how='left')

		# 3.2) Number of Transfers
		tmp = transactions.groupby(['linked_uid']).agg({'Transfer':'sum'}).reset_index().rename(columns={'Transfer':'n_transfers'})
		expanded_linked_trips = expanded_linked_trips.merge(tmp, on='linked_uid', how='left')

		# 3.3) Trip Itinerary

		# Read in route names
		route_names = pd.read_csv(os.path.join(self.config['data_dir'], 'output', 'Tableau', 'route_names.csv'))

		# Sort transctions & add route names
		trans = transactions.sort_values(['holocard_uid', 'tap_datetime']).reset_index() \
		    .merge(route_names, on=['route_id'], how='left')

		# Create Itinerary attribute
		itinerary = trans.groupby(['linked_uid'])['route_name'].apply(list).reset_index()
		itinerary['itinerary'] = itinerary['route_name'].apply(lambda x: ' >> '.join(x))

		# Create Lookup table with Itinerary index
		itinerary_lookup = itinerary[['itinerary']].drop_duplicates().reset_index(drop=True)
		itinerary_lookup['itinerary_id'] = range(itinerary_lookup.shape[0])

		# Write out lookup table
		itinerary_lookup.to_csv(os.path.join(self.config['data_dir'], 'output', 'Tableau', 'itinerary_id.csv'), index=None)

		# Add itinerary_id to Linked Trips df
		itinerary = itinerary.merge(itinerary_lookup, on=['itinerary'])[['linked_uid', 'itinerary_id']]
		expanded_linked_trips = expanded_linked_trips.merge(itinerary, on=['linked_uid'])

		# 3.4) O-D Districts
		stops = gpd.read_file(os.path.join(self.config['data_dir'], 'output', 'Tableau', 'geo', 'stops.shp'))
		stop_district_map = dict(zip(stops['stop_code'], stops['District']))
		expanded_linked_trips['District - Origin'] = expanded_linked_trips['stop_code - Origin'].map(stop_district_map)
		expanded_linked_trips['District - Destination'] = expanded_linked_trips['stop_code - Destination'].map(stop_district_map)

		# Handle NA
		assert expanded_linked_trips['District - Origin'].isna().sum()==0
		expanded_linked_trips['District - Destination'].fillna(-1, inplace=True)
		expanded_linked_trips['District - Origin'] = expanded_linked_trips['District - Origin'].astype(int)
		expanded_linked_trips['District - Destination'] = expanded_linked_trips['District - Destination'].astype(int)


		# 3.5) O-D Boards
		stop_board_map = stops[['stop_code', 'Board']]
		expanded_linked_trips = expanded_linked_trips.merge(stop_board_map, left_on=['stop_code - Origin'], right_on=['stop_code'], how='left') \
		        .rename(columns={'Board':'Board - Origin'}).drop(columns=['stop_code']) \
		    .merge(stop_board_map, left_on=['stop_code - Destination'], right_on=['stop_code'], how='left') \
		        .rename(columns={'Board':'Board - Destination'}).drop(columns=['stop_code'])

		# Handle NA
		assert expanded_linked_trips['Board - Origin'].isna().sum()==0
		expanded_linked_trips['Board - Destination'].fillna(-1, inplace=True)
		expanded_linked_trips['Board - Origin'] = expanded_linked_trips['Board - Origin'].astype(int)
		expanded_linked_trips['Board - Destination'] = expanded_linked_trips['Board - Destination'].astype(int)


		## 4. Write Out
		
		# QA/QC
		assert expanded_linked_trips['weight'].sum()==n

		pantab.frame_to_hyper(expanded_linked_trips, os.path.join(self.config['data_dir'], 'output', 'Tableau', 'linked_Trips_Tableau.hyper'), table="Linked Trips")


def main():
	tableauPostProcessing = TableauDataPostprocessing()
	tableauPostProcessing.geo_data()
	tableauPostProcessing.transactions()
	tableauPostProcessing.user_segments()
	tableauPostProcessing.expanded_linked_trips() 



if __name__ == "__main__":
	main()