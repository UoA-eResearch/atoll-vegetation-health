import ee

try:
    ee.Initialize()
except:                
    ee.Authenticate()
    ee.Initialize()

from geeutil import feature_utils
from geeutil import sentinel2_utils
from geeutil import image_utils
from geeutil import normalised_difference
from geeutil import h3_utils 
from geeutil import imagecollection_utils

import geopandas as gpd
import pandas as pd
import time

from tqdm.contrib.concurrent import process_map
from functools import partial
import glob
import os
from datetime import datetime as dt

### FUNCTIONS ###
def return_mean_ndvi(geometry, buffer_distance=None): # function to return mean ndvi for geometry
    """
    Calculate mean ndvi for all images in ee.ImageCollection with map fuction. 

    Args
    geometry - ee.Geometry object representing area
    buffer_distance - distance to buffer geometry to reduce mixed pixels
    """
    if buffer_distance != None:
            geometry = geometry.buffer(buffer_distance)
    def get_stats(image):
        
        indices = image.select(['ndvi'])
        stats = indices.reduceRegion(**{
            'geometry': geometry,
            'reducer': ee.Reducer.mean()
        })
        ndvi = stats.get('ndvi')

        feature = ee.Feature(None, {
            # add indices
            'ndvi': ndvi
        }).copyProperties(image, [
            'system:time_start' # get image acquisition 
        ])
        return feature
    return(get_stats)

def process_images(feature, image_collections, folder_path): # process all images in imageCollection
    """
    return mean ndvi for all images an a series of imageCollections, 
    returning csv of ndvi timeseries
    
    Args
    feature - geometry of area for which ndvi is being calculated
    image_collections - dictionary containing year: 'sensorID'
    folder_path - output directory path 
    """
    aoi = feature_utils.item_to_featureCollection(feature) # return cell as ee.featureCollection
    timeseries = []
    for year, sensor in image_collections.items(): # iterate through imageCollections
        images = imagecollection_utils.gen_imageCollection(year, aoi, sensor)
        if images.size().getInfo() != 0:
                images = (images.map(normalised_difference.apply_ndvi))
                veg_stats = images.map(return_mean_ndvi(aoi))
                features = [i for i in veg_stats.getInfo().get('features')]
                for f in features:
                    #print(f)
                    image_attributes = {} 
                    date = dt.fromtimestamp(f['properties']['system:time_start']/ 1000.)
                    try:
                        image_attributes['date'] = date # return attributes for each image
                        image_attributes['sensor'] = sensor
                        image_attributes['image_id'] = f['id']
                        image_attributes['ndvi'] = f['properties']['ndvi']

                        timeseries.append(image_attributes)
                    except:
                        #print(f"Issue with image id: {f['id']} from {year}")
                        #print(f"skipping {f['id']}")
                        pass
        else:
            print(f"No images for shp_id: {feature['properties']['index']}
                    sensor: {sensor}
                    year: {year}")
            continue
    
    fn = f"{folder_path}/{feature['properties']['index']}-timeseries.csv"
    df = pd.DataFrame.from_dict(timeseries, orient='columns')
    df.to_csv(fn)
### END OF FUNCTIONS ###


image_collections = {} # define sensors and years for imageCollections

for i in range(1984, 2012):
    image_collections[i] = 'LS5'
for i in range(1999, 2003, 1):
    image_collections[i] = 'LS7'
for i in range(2013, 2023, 1):
    image_collections[i] = 'HLSL30'

output_directory_path = "test-data"

if __name__ == '__main__':
    start = time.time()

    # shapefiles/geometries go here 
    h3_grids = gpd.read_file("test-data/h3-coast-all-res.gpkg")
    hr5_cells = gpd.read_file("test-data/HR5-change-cells-aoi.gpkg")

    geometries = list(hr5_cells[:1]['index']) # list of geometries for iteration

    for geom in geometries:
        geom_output_dir = os.path.join(output_directory_path, geom)
        if not os.path.exists(geom_output_dir):
            os.makedirs(geom_output_dir)
            # return all child cells at res 5 for test cell at index 4
        down_cells = h3_utils.get_child_cells(h3_grids, geom, 6)

        print(down_cells)

        # test downloading whole cell at HR5
        #down_cells = h3_cells[h3_cells['index'] == i]
        # reproject to 4326 for GEE
        down_cells.to_crs(4326, inplace=True)

        # create dict of cells to be downloaded
        features = down_cells.iterfeatures()

        # download images
        process_map(partial(process_images, image_collections=image_collections, folder_path=geom_output_dir), features)

    end = time.time()
    print(f"Processing finished in {(end-start)/60} minutes.")
