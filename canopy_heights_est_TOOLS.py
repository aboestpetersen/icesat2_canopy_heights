'''
Author: Alexander Boest-Petersen, 2021
'''

import glob
import os
import sys
import urllib
import zipfile
from pathlib import Path

import geopandas as gpd
import icepyx as ipx
import pandas as pd
from pandas.core.reshape.reshape import stack_multiple
from tqdm.notebook import tqdm_notebook

# Import PhoREAL 'getAtlMeasuredSwath' tool
sys.path.insert(1, 'C:/Users/albp/OneDrive - DHI/Documents/GitHub/icesat2_canopy_heights/PhoREAL-master/source_code/')
from getAtlMeasuredSwath_auto import getAtlMeasuredSwath

'''
Download ICESat-2 Data from NSIDC.
'''
def download_icesat(data_type = 'ATL03', spatial_extent = False, date_range = False, username = False, email = False, output_location = False):
    # Only execute code if input ATL03.h5 filepath and outpath declared
    if (output_location):

        # Create download directory if necessary
        Path(output_location).mkdir(parents=True, exist_ok=True)

        # Download ICESat-2 files
        region_a = ipx.Query(data_type, spatial_extent, date_range)
        region_a.earthdata_login(username, email)
        region_a.order_granules()
        region_a.granules.orderIDs
        region_a.download_granules(output_location)
        print('Done.')

    else:
        print('No storage directory given.')

'''
TODO: DEFINE WHAT THIS TOOL DOES.
'''
def get_canopy_heights(download = False, data_type = 'ATL03', spatial_extent = False, date_range = False, username = False, email = False, atl03FileLocation = False, trackNum = ['gt1l', 'gt2l', 'gt3l', 'gt1r', 'gt2r', 'gt3r'], alongTrackRes = 10):
    
    # Only execute code if input ATL03.h5 filepath and outpath declared
    if (atl03FileLocation):

        # Create download directory if necessary
        Path(atl03FileLocation).mkdir(parents=True, exist_ok=True)

        # Download ICESat-2 files if requested
        if download == True:
            region_a = ipx.Query(data_type, spatial_extent, date_range)
            region_a.earthdata_login(username, email)
            region_a.order_granules()
            region_a.granules.orderIDs
            region_a.download_granules(atl03FileLocation)
        else:
            print('Not downloading raw files from NSIDC.')
            pass

        # Locate available .h5 files for processing
        atl03Files = glob.glob(os.path.join(atl03FileLocation, f'*.h5'))
        # Print file names
        print('Found {} ATL03 file(s).'.format(len(atl03Files)))

        # Create storage location for converted files
        output_location = atl03FileLocation + 'csv/'
        Path(output_location).mkdir(parents=True, exist_ok=True)

        # Process .h5 files with PhoREAL tool to convert to csv with return statistics
        for files in tqdm_notebook(atl03Files):
            for track in trackNum:
                getAtlMeasuredSwath(atl03FilePath = files, outFilePath = output_location, gtNum = track, trimInfo = 'auto', createAtl03CsvFile = True)
        
        print('Finished converting to csv.')

        # Locate available csv files for analysis
        #csvFiles = glob.glob(os.path.join(storePath, f'*'+trackNum+'.csv')) # Original
        csvFiles = glob.glob(os.path.join(output_location, f'*.csv'))
        print('Located {} csv file(s) for canopy heights estimation.'.format(len(csvFiles)))

        # Create storage directory for heights if necessary
        csvPath = output_location+'/CANOPY_ESTIMATES/'
        Path(csvPath).mkdir(parents=True, exist_ok=True)

        # Confirm along-track resolution
        print('Canopy heights will be generated at a resolution of {} meters along-track.'.format(alongTrackRes))

        # Create empty 'merged' dataframe to concat other dataframes to
        merged_df = pd.read_csv(csvFiles[0], nrows=0)

        # Estimate ground and canopy returns from raw ATL03 data
        for csvs in tqdm_notebook(csvFiles):

            # Obtain .h5 file name
            print('Processing: {}.'.format(csvs))

            # Load CSV file into dataframe
            df = pd.read_csv(csvs)

            # Sort out errorneous datasets by locating dataframes with 'NaN' values.
            while True:
                try:
                    nbins = int(((df['Along-Track (m)'].max()) - (df['Along-Track (m)'].min()))/alongTrackRes)

                    # Filter data to med. & high signal confidence for ground estimates
                    df_medhigh = df[df['Signal Confidence'] > 2]

                    # Calculate # of bins required for given resolution
                    nbins = int(((df_medhigh['Along-Track (m)'].max()) - (df_medhigh['Along-Track (m)'].min()))/alongTrackRes)

                    # Sort dataframe in ascending order based on bins
                    df_medhigh.sort_values('Along-Track (m)', inplace=True)

                    # Create binned dataframe
                    df_ground = df_medhigh.copy()
                    df_ground['bin'] = pd.cut(df_medhigh['Along-Track (m)'], bins=nbins, include_lowest=True)

                    # Located lowest elevation of each bin to define as the 'ground'
                    df_ground['ground_est'] = df_ground.groupby('bin')['Height (m MSL)'].transform('min')
                    print('Ground returns estimated for {}.'.format(csvs))

                    # Filter data to x classes for canopy estimations
                    df_canopy = df[df['Signal Confidence'] > 0]

                    # Create binned canopy dataframe
                    df_canopy['canopy_bin'] = pd.cut(df_canopy['Along-Track (m)'], bins=nbins, include_lowest=True)

                    # Locate and define max return height for each bin
                    df_canopy['toc_est'] = df_canopy.groupby('canopy_bin')['Height (m MSL)'].transform('max')

                    # Remove canopy estimates that fall below ground level
                    df_canopy['elev_bin'] = pd.cut(df_canopy['Along-Track (m)'], bins=nbins, include_lowest=True)

                    df_canopy['ground_est'] = df_canopy.groupby('elev_bin')['Height (m MSL)'].transform('min')

                    # Aggregate columns by bin and mean value
                    d_mean = df_canopy.groupby('canopy_bin').agg(['mean'])
                    # Flatten columns
                    d_mean.columns = d_mean.columns.map('_'.join)
                    # Revert back to proper dataframe
                    d_mean.reset_index()

                    # Calculate height of canopy above ground
                    d_mean['canopy_height_est'] = d_mean['toc_est_mean'] - d_mean['ground_est_mean']

                    # Remove rows with no coordinates
                    d_mean.dropna(subset=['Longitude (deg)_mean'], inplace=True)

                    # Add track and year columns
                    year = csvs[-39:-35]
                    trackID = csvs[-8:-4]
                    d_mean['year'] = year
                    d_mean['track'] = trackID
                
                    # Concat final dataframe to merged set
                    merged_df = pd.concat([merged_df, d_mean])
                    print('Merged csv has {} entrys.'.format(len(merged_df)))
                    break
                
                except ValueError:
                    print('Erroneous data, skipping.')
                    break

        # Drop empty columns
        merged_df.drop(merged_df.columns[[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]], axis=1, inplace=True)

        # Rename Lat & Lon columns for GEE asset ingestion
        merged_df.rename(columns={'Latitude (deg)_mean':'latitude', 'Longitude (deg)_mean':'longitude'}, inplace=True)
        
        # Save merged dataframe as csv
        merged_df.to_csv(csvPath+'canopy_merged.csv', sep=',', index=False)
        print('Derived and saved merged canopy heights.')

    else:
        print('ERROR: No .h5 files found in specified location and/or no output directory specified.')

'''
Tool to clip GMW 2016 dataset to area of interest and generate files for GEE processing.
GMW_2016 .shp files can be found here:
    https://data.unep-wcmc.org/datasets/45

This tool generates 3 shapefiles:
    1. Areas of known mangroves (as per GMW) within the designated aoi.
    2. Areas that are not mangroves within the designated aoi.
    3. Combined areas of known and non mangroves for designated aoi. (remove?)
'''

'''
TODO: Check if study_area_name is valid (no spaces, etc.)
'''
def gmw_mangroves(gmw2016_path = False, spatial_extent = False, study_area_name = False):

    if (gmw2016_path and spatial_extent and study_area_name):
            
        # Locate available .shp files for processing
        gmw_polygons = glob.glob(os.path.join(gmw2016_path, f'*GMW_2016_v2.shp'))

        # Obtain directory for file consumption
        directory = os.getcwd()

        # Download & unzip shapefiles if none are found
        while True:
            if len(gmw_polygons) == 0:
                url = 'https://wcmc.io/GMW_2016'
                file = 'GMW_001_GlobalMangroveWatch_2016.zip'
                print('No GMW shapefiles found. Downloading 2016 shapefiles...')
                
                # Download GMW 2016 shapefiles
                urllib.request.urlretrieve(url, gmw2016_path+file)

                # Unzip compressed files
                with zipfile.ZipFile(gmw2016_path+file,'r') as zip_ref:
                    zip_ref.extractall(gmw2016_path)
                
                # Set file path to new location
                gmw2016_path = gmw2016_path + '/GMW_001_GlobalMangroveWatch_2016/01_Data'

                # Locate newly downloaded files
                gmw_polygons = glob.glob(os.path.join(gmw2016_path, f'*GMW_2016_v2.shp'))

                # Load shapefile into geopandas dataframe
                gmw_polygons = gpd.read_file(directory+'/'+gmw_polygons[0])
                print('Downloaded GMW 2016 shapefiles that can be found at: {}'.format(gmw_polygons))
                break
            else:
                # Load shapefile into geopandas dataframe
                gmw_polygons = gpd.read_file(directory+'/'+gmw_polygons[0])
                print('Located shapefile.')
                break

        # Read AOI shapefile
        aoi = gpd.read_file(spatial_extent)

        # Clip GMW shapefiles by aoi area
        gmw_clipped = gmw_polygons.clip(aoi)
        gmw_clipped.head()

        '''
        # Define special characters for checking storage location
        special_characters = '"[@_!#$%^&*()<>?/\|}{~:]"'

        # Check nomenclature of study area name (no spaces allowed, may not start with a number)
        while True:
            if ' ' in study_area_name:
                print('Space found in study_area_name variable. Change this.')
                sys.exit()
            elif str(study_area_name[0]).isdigit:
                print('First character of study_area_name is a number. Change this.')
                sys.exit()
            elif any(c in special_characters for c in study_area_name):
                print('Special character found in study_area_name. Change this.')
                sys.exit()
            else:
                print('No spaces found.')
                break
        '''
    else:
        print('ERROR: No .shp files found in specified location and/or incorrect directory specified.')
