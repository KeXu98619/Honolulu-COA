from gtfs_module import gtfs_module
from preprocessing_module import preprocessing
from generate_linked_trips_module import generate_linked_trips
from expansion_module import expansion_module
#from tableau_data_postprocessing import tableau_data_postprocessing
import time

# Run all modules.

# 1. GTFS
print('GTFS...')
start = time.time()
gtfs_module.main()
print(f"Finished in {round((time.time()-start)/60, 2)} minutes.\n---------\n")

# 2. Preprocessing
print('Preprocessing...')
start = time.time()
preprocessing.main()
print(f"Finished in {round((time.time()-start)/60, 2)} minutes.\n---------\n")

# 3. Generate Linked Trips
print('Generate Linked Trips...')
start = time.time()
generate_linked_trips.main()
print(f"Finished in {round((time.time()-start)/60, 2)} minutes.\n---------\n")

# 4. Expansion
print('Expansion...')
start = time.time()
expansion_module.main()
print(f"Finished in {round((time.time()-start)/60, 2)} minutes.\n---------\n")

# 5. Tableau Data Postprocessing
print('Tableau Data Postprocessing...')
start = time.time()
tableau_data_postprocessing.main()
print(f"Finished in {round((time.time()-start)/60, 2)} minutes.\n---------\n")