
# Modules
The HOLO ODX model involves running six modules in sequence:  
1. GTFS Module  
2. Preprocessing Module  
3. Generate Linked Trips Module  
4. Expansion Module  
5. Tableau Data Postprocessing Module


## 2.1 GTFS Module 

#### Summary  
For the temporal extent of HOLO data, create relevant GTFS reference files given GTFS feeds that cover that span. These can be run via the `control.py` script included in the package. 

#### Input  
1. **GTFS feeds** – GTFS Feeds that cover the range of the input dates (`data/input/gtfs`)  
2. `config_gtfs.yaml` – Configuration file that specifies the service day range for which to process GTFS data 

#### Output 
1. `gtfs_feed_info.csv` – Mapping of service day to the gtfs feed to use for that service day
2. `gtfs_nearest_stop.csv` -- For every GTFS feed + stop combination, find the nearest stop for every route in that GTFS feed
3. `gtfs_routes.csv` – Route details (ID, name, stops) for every route  
4. `gtfs_stops.csv` – Stop details (ID, stop name, lat/lon) for every stop  
5. `gtfs_stop_to_stop_dist.csv` – For every GTFS Feed + stop combination, get the distance between stops


## 2.2 Preprocessing Module

#### Summary
Preprocess (normalize, recode attributes, drop invalid records, etc.) HOLO & APC data

#### Input
1. **HOLO data** – Raw HOLO data
2. **APC data** – Raw APC data
3. **Configuration Files** – one for each of APC & HOLO

#### Output 
1. `holo_processed.csv`
2. `apc_processed.csv`
3. **lookup tables** – lookup tables for indexed Holo attributes


## 2.3 Generate Linked Trips Module

##### Summary
Convert transaction-level HOLO data to linked trips using assumptions specified in configuration file. 

#### Input
1. `config_linked_trips.yaml` – Specify linked trip assumptions, such as destination inference rounds, transfer time threshold, etc. See file for all hyperparameters
2. `holo_processed.csv` – Cleaned HOLO file; output from the Preprocessing module
3. `gtfs_nearest_stop.csv` – For transfer inference; output from the GTFS module
4. `gtfs_stop_to_stop_dist.csv` – For destination inference; output from the GTFS module

#### Output
1. `holo_linked_trips.csv`

## 2.4 Expansion Module

#### Summary
1. Expand the HOLO linked trips module to the APC totals.

#### Input 
1. `config_expansion_module.yaml` – Specify similar day definitions for use in imputing holo records. 
2. `holo_linked_trips.csv` – Output from Linked Trips Module
3. `apc_processed.csv` – Output from Preprocessing Module

#### Output
1. `expanded_linked_trips.csv`


## 2.5 Tableau Data Postprocessing Module

#### Summary
1. Create files (geo, linked trip & transactions .hyper files, user segmentations) for the Tableau dashboard.  

#### Input
1. `config_Tableau_post_processing.yaml` – Define data paths and User Segmentation hyperparameters  
2. `Neighborhood_Boards.shp` – Shapefile defining the 34 Honolulu Neighborhood Boards  
3. `board_names.csv` – Mapping of board ID to board names  
4. `DPA.shp` – Shapefile defining the 9 Honolulu districts   
5. `gtfs_stops.shp` – Output from GTFS Module  
6. `gtfs_routes.shp` – Output from GTFS Module  
7. `expanded_linked_trips.csv` – Output from Expansion Module  
8. `holo_processed.csv` – Output from Expansion Module  

#### Output
1. `boards.shp` – Boards shapefile  
2. `district.shp` – District shapefile  
3. `stops.shp` – Bus stop shape file  
4. `route_names.csv` – Lookup file mapping route ID to route name  
5. `user_segmentations.csv` – User Segmentation  
6. `itinerary_id.csv` – Lookup file mapping itinerary ID to verbose itinerary name  
7. `linked_Trips_Tableau.hyper` – Modified output of Expanded Linked Trip Module in .hyper format.   
8. `transactions_Tableau.hyper` – Modified output of processed HOLO in .hyper format.  

