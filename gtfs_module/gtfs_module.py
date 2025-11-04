import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
import yaml
import ast

# Set path to enable module imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# Set up logger
import utility_module.logger as logging 
logger = logging.get_logger('__name__','../../log_files/gtfs_module.log')


class GTFS:
	"""
	Class that reads in n GTFS feeds & writes out three tables:
		1. gtfs_info_feed.csv	  : Determines which GTFS feed to use for each service day (provided in config). 
		2. stops.csv			  : Stop attributes (code, serial_number, name, lat/lon) for each stop across all GTFS feeds.
		3. routes.csv			  : Route attributes (id, name, stops in route) for each route across all GTFS feeds.
	"""

	def __init__(self):

		# Config File
		try:
			with open(os.path.join("config_files", "config_gtfs.yaml"), "r") as f:
				self.config = yaml.safe_load(f)
		except:
			logger.exception("Could not read config file")


	def create_feed_info_table(self):
		"""
		For each service date, determine which GTFS feed to use. 
		"""

		logger.info("Creating Feed Info table...")

		### Combine feed_info.txt files of each available GTFS feed
	
		feed_info = []
		for file in [f for f in os.listdir(self.config['gtfs_path'])]:
			feed_info.append(pd.read_csv(os.path.join(self.config['gtfs_path'], file, 'feed_info.txt'))[['feed_version', 'feed_start_date', 'feed_end_date']])
		feed_info = pd.concat(feed_info).reset_index(drop=True)
		feed_info['feed_version'] = feed_info['feed_version'].str.replace('.','',regex=False).astype(int)
		feed_info['feed_start_date'] = pd.to_datetime(feed_info['feed_start_date'], format="%Y%m%d")
		feed_info['feed_end_date'] = pd.to_datetime(feed_info['feed_end_date'], format="%Y%m%d")
		feed_info.rename(columns={'feed_version':'gtfs_feed'}, inplace=True)
		
		### For each unique service date, determine GTFS feed to use
		
		start_date = datetime.strptime(self.config['start_service_day'], "%Y-%m-%d").date()
		end_date = datetime.strptime(self.config['end_service_day'], "%Y-%m-%d").date()
		dates = pd.date_range(start_date, end_date-timedelta(days=1), freq='d').to_frame().reset_index(drop=True) \
			.rename(columns={0:'service_day'})
		dates['tmp'] = 1
		feed_info['tmp'] = 1
		date_to_feed = pd.merge(dates, feed_info, on=['tmp']).drop(columns=['tmp'])
		date_to_feed = date_to_feed[
			(date_to_feed['service_day'] >= date_to_feed['feed_start_date']) & (date_to_feed['service_day'] <= date_to_feed['feed_end_date'])
		].reset_index(drop=True)
		
		# For dates with multiple GTFS feed matches, use the more recent feed
		date_to_feed = date_to_feed[date_to_feed.groupby(['service_day'])['gtfs_feed'].transform(max)==date_to_feed['gtfs_feed']] \
			.reset_index(drop=True)
		
		# Make sure all dates have exactly ONE GTFS version
		assert len(set(date_to_feed['service_day']) - set(dates['service_day']))==0
		
		# Keep only relevant columns
		date_to_feed = date_to_feed[['service_day', 'gtfs_feed']]

		### Write out
		date_to_feed.to_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_feed_info.csv'), index=None)


	def create_routes_table(self):

		logger.info("Creating Routes table...")
		
		routes_all = []

		for file in [f for f in os.listdir(self.config['gtfs_path'])]:

			### Read in relevant files
			feed_info = pd.read_csv(os.path.join(self.config['gtfs_path'], file, 'feed_info.txt'))
			routes = pd.read_csv(os.path.join(self.config['gtfs_path'], file, 'routes.txt'), dtype={'route_id': str})
			trips = pd.read_csv(os.path.join(self.config['gtfs_path'], file, 'trips.txt'), dtype={'route_id': str})
			stop_times = pd.read_csv(os.path.join(self.config['gtfs_path'], file, 'stop_times.txt'), dtype={'stop_id': str})
			stops = pd.read_csv(os.path.join(self.config['gtfs_path'], file, 'stops.txt'), dtype={'stop_id': str})  
			
			### Process GTFS Routes
			routes_tmp = routes[['route_id', 'route_short_name', 'route_long_name']] \
				.merge(trips[['route_id', 'trip_id']], on='route_id', how='inner') \
				.merge(stop_times[['trip_id', 'stop_id', 'stop_sequence']], on='trip_id', how='left') \
				.merge(stops[['stop_id', 'stop_code']], on='stop_id', how='left').drop(columns=['stop_id'])

			# Create routes table
			routes_tmp = routes_tmp.groupby(['route_id', 'route_short_name', 'route_long_name'])['stop_code'].apply(set).to_frame('stop_codes').reset_index()
			routes_tmp['gtfs_feed'] = int(feed_info.iloc[0]['feed_version'].replace('.',''))
			
			assert routes_tmp.isna().sum().sum()==0
			
			### Add DFs to main
			routes_all.append(routes_tmp)

		# Combine
		routes_all = pd.concat(routes_all)[['gtfs_feed', 'route_id', 'route_short_name', 'route_long_name', 'stop_codes']]
		# Remove null routes
		routes_all = routes_all[routes_all['stop_codes'].apply(lambda x : x != set([np.nan]))].reset_index(drop=True)
		# Change dtypes
		routes_all['route_id'] = routes_all['route_id'].astype(int)

		# QA/QC -- ensure each gtfs_feed + route_id + route_short_name is represented EXACTLY ONCE
		assert routes_all.groupby(['gtfs_feed', 'route_short_name', 'route_long_name', 'route_id']).size().max() == 1

		### Write out
		routes_all.to_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_routes.csv'), index=None)


	def create_stops_table(self):

		logger.info('Creating Stops table...')

		stops_all = []

		for file in [f for f in os.listdir(self.config['gtfs_path'])]:

			### Read in relevant files
			feed_info = pd.read_csv(os.path.join(self.config['gtfs_path'], file, 'feed_info.txt'))
			stops = pd.read_csv(os.path.join(self.config['gtfs_path'], file, 'stops.txt'))

			# Add GTFS Feed
			stops['gtfs_feed'] = int(feed_info.iloc[0]['feed_version'].replace('.',''))

			# Keep only relevant columns
			stops = stops[['gtfs_feed', 'stop_code', 'stop_serial_number', 'stop_name', 'stop_lat', 'stop_lon']]

			# If duplicates, take first stop_code available. 
			stops = stops.groupby(['gtfs_feed', 'stop_code', 'stop_serial_number']).first().reset_index()

			# QA/QC -- ensure each stop_code & stop_serial_number are represented only once
			assert stops.groupby(['stop_code']).size().max()==1
			assert stops.groupby(['stop_serial_number']).size().max()==1

			### Add DFs to main
			stops_all.append(stops)

		# Combine
		stops_all = pd.concat(stops_all)

		# QA/QC -- ensure each gtfs_feed + route_id + route_short_name is represented EXACTLY ONCE
		assert stops_all.groupby(['gtfs_feed', 'stop_code', 'stop_serial_number']).size().max() == 1

		### Write out
		stops_all.to_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_stops.csv'), index=None)


	def create_nearest_stop_lookup_table(self):
		"""
		For every gtfs_feed + stop_code combination, find the nearest stop for every route_id in that gtfs_feed.
		"""

		logger.info("Creating Nearest Stop Lookup table...")

		try:
			gtfs_stops = pd.read_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_stops.csv'))
			gtfs_routes = pd.read_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_routes.csv'))
		except:
			logger.info('Cannot read GTFS Stops or Routes table.')


		## Restructure files

		# STOPS
		stops = gpd.GeoDataFrame(gtfs_stops, geometry=gpd.points_from_xy(gtfs_stops['stop_lon'], gtfs_stops['stop_lat']), crs='epsg:4326') \
		    [['gtfs_feed', 'stop_code', 'geometry']]
		stops = stops.to_crs(epsg=3763)
		assert stops.crs.axis_info[0].unit_name == 'metre'

		# ROUTES
		routes = []
		for i, row in gtfs_routes.iterrows():
		    
		    r = pd.DataFrame({'stop_code':list(ast.literal_eval(row['stop_codes']))})
		    r['stop_code'] = r['stop_code'].astype(int)
		    r['route_id'] = row['route_id']
		    r['gtfs_feed'] = row['gtfs_feed']
		    routes.append(r)

		routes = pd.concat(routes)
		routes = routes.merge(gtfs_stops, on=['gtfs_feed', 'stop_code'], how='left') \
		    [['gtfs_feed', 'route_id', 'stop_code', 'geometry']].rename(columns={'stop_code':'nearest_stop'})
		routes = gpd.GeoDataFrame(routes, geometry=routes['geometry'], crs='epsg:4326')
		routes = routes.to_crs(epsg=3763)
		assert routes.crs.axis_info[0].unit_name == 'metre'

		## Create lookup table

		# Merge
		df = routes.merge(stops, on='gtfs_feed')

		# Calculate distance between stops (in miles)
		df['dist'] = gpd.GeoSeries.distance(
		    gpd.GeoSeries(df['geometry_x']), gpd.GeoSeries(df['geometry_y']))
		df['dist'] *= 0.000621371

		# Keep only relevant columns
		df = df[['gtfs_feed', 'stop_code', 'route_id', 'nearest_stop', 'dist']]

		# Keep min distance 
		df = df[df.groupby(['gtfs_feed', 'stop_code', 'route_id'])['dist'].transform(min) == df['dist']]

		# Ensure only one nearest stop per combo
		assert df.groupby(['gtfs_feed', 'stop_code', 'route_id']).size().max()==1

		## Write out

		df.to_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_nearest_stop.csv'), index=None)


	def create_stop_to_stop_dist_table(self):
		"""
		For every gtfs_feed + stop_code combination, get the distance between stops.
		"""

		logger.info("Creating Stop to Stop Distance table...")

		# Read in stops data
		try:
			gtfs_stops = pd.read_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_stops.csv'))
		except:
			logger.info('Cannot read GTFS Stops or Routes table.')

		# Convert to GeoDataFrame
		stops = gpd.GeoDataFrame(gtfs_stops, geometry=gpd.points_from_xy(gtfs_stops['stop_lon'], gtfs_stops['stop_lat']), crs='epsg:4326') \
		    [['gtfs_feed', 'stop_code', 'geometry']]
		stops = stops.to_crs(epsg=3763)
		assert stops.crs.axis_info[0].unit_name == 'metre'

		# Create stops<>stops by GTFS Feed
		stops = stops.merge(stops, on=['gtfs_feed'], how='outer')

		# Calculate distance between stops (in miles)
		stops['dist'] = gpd.GeoSeries.distance(
		    gpd.GeoSeries(stops['geometry_x']), gpd.GeoSeries(stops['geometry_y']))
		stops['dist'] *= 0.000621371

		# Keep only relevant columns
		stops = stops[['gtfs_feed', 'stop_code_x', 'stop_code_y', 'dist']]

		# Write out
		stops.to_csv(os.path.join(self.config['out_dir'], 'gtfs', 'gtfs_stop_to_stop_dist.csv'), index=None)


def main():
	gtfs = GTFS()
	gtfs.create_feed_info_table()
	gtfs.create_stops_table()
	gtfs.create_routes_table()
	gtfs.create_nearest_stop_lookup_table()
	gtfs.create_stop_to_stop_dist_table()


if __name__ == "__main__":
	main()
