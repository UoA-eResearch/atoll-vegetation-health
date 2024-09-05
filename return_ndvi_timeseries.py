from geeutil import feature_utils
from geeutil import sentinel2_utils
from geeutil import image_utils
from geeutil import normalised_difference
from geeutil import h3_utils 
from geeutil import imagecollection_utils

import geopandas as gpd
import pandas as pd
import ee
import time

from tqdm.contrib.concurrent import process_map
from functools import partial
import glob
import os
from datetime import datetime as dt

ee.Initialize()

### FUNCTIONS ###
def return_mean_ndvi(geometry, buffer_distance=None): # function to return mean ndvi for geometry
    """
    Calculate mean ndvi for all images in ee.ImageCollection with map fuction. 

    Args
    geometry - ee.Geometry object representing area
    buffer_distance - distance to buffer geometry to reduce mixed pixels
    """
    def get_stats(image):
        if buffer_distance != None:
            geometry = geometry.buffer(buffer_distance)
        indices = image.select(['ndvi'])
        stats = indices.reduceRegion(**{
            'geometry': geometry,
            'reducer': ee.Reducer.mean()
        })
        ndvi = stats.get('ndvi')

        feature = ee.Feature(None, {
            # add indices
            'NDVI': ndvi
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
            try:
                images = (images.map(normalised_difference.apply_ndvi))
                veg_stats = images.map(return_mean_ndvi(aoi))
                features = [i for i in veg_stats.getInfo().get('features')]
                for f in features:
                    image_attributes = {} 
                    date = dt.fromtimestamp(f['properties']['system:time_start']/ 1000.)
                    
                    image_attributes['date'] = date # return attributes for each image
                    image_attributes['sensor'] = sensor
                    image_attributes['image_id'] = f['id']
                    image_attributes['ndvi'] = f['properties']['NDVI']

                    timeseries.append(image_attributes)
            except:
                print(f"There was an issue with {sensor} for year: {year}")
                pass
        else:
            print(f"No images for {sensor} in year: {year}")
            continue
    
    fn = f"{folder_path}/{feature['properties']['index']}-timeseries.csv"
    df = pd.DataFrame.from_dict(timeseries, orient='columns')
    df.to_csv(fn)
### END OF FUNCTIONS ###
# define sensors and years for imageCollections
image_collections = {

}

output_directory_path = "data"

if __name__ == '__main__':
    start = time.time()

    # shapefiles/geometries go here 

    geometries = list() # list of geometries for iteration

    for geom in geometries:
        geom_output_dir = os.path.join(output_directory_path, geom)
        if not os.path.exists(geom_output_dir):
            os.makedirs(geom_output_dir)


    end = time.time()
    print(f"Processing finished in {(end-start)/60} minutes.")