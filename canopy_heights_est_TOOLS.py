'''
Author: Alexander Boest-Petersen, 2022
TODO: Documentation.
TODO: Clean up imports.
TODO: Plot canopy estimations.
TODO: GEDI Implementation?
'''

from getAtlMeasuredSwath_auto import getAtlMeasuredSwath
import glob
import os
#import sys
import urllib
import zipfile
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import icepyx as ipx
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
#from pandas.core.reshape.reshape import stack_multiple
#from shapely.validation import make_valid
from tqdm.notebook import tqdm_notebook

# Import PhoREAL 'getAtlMeasuredSwath' tool
# print(os.getcwd())
# Figure out how to clean this up.
os.chdir('C:/Users/albp/OneDrive - DHI/Documents/GitHub/icesat2_canopy_heights/PhoREAL/source_code/')

# Get todays date for downloading data
today = datetime.today().strftime('%Y-%m-%d')


def download_icesat(
        data_type='ATL03',
        spatial_extent=False,
        date_range=[
            '2018-10-13',
            today],
    username=False,
    email=False,
        output_location=False):
    '''
    Description:
        Query & download ICESat-2 Data from NSIDC API using icepyx.
    Parameters:
        data_type - Which dataset to download from NSIDC.
        spatial_extent - The extent for which to download ICESat-2 data from.
        date_range - Date range for download of ICESat-2 data.
        username - NASA EarthData username.
        email - NASA EarthData email.
        output_location - Directory to store downloaded files.

    TODO: Documentation
    '''
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


def get_canopy_heights(
        download=False,
        data_type='ATL03',
        spatial_extent=False,
        date_range=[
            '2018-10-13',
            today],
    username=False,
    email=False,
    working_directory=False,
    generate_csv=True,
    track_num=[
            'gt1l',
            'gt2l',
            'gt3l',
            'gt1r',
            'gt2r',
            'gt3r'],
        along_track_res=10,
        autocorrelation=False,
        autocorrelation_dist=250):
    '''
    Description:
        get_canopy_heights is a tool that allows users to query, download
        preprocess, and obtain estimated canopy heights from NASA's ATLAS
        ICESat-2 ATL03 raw photon return data at a user defined resolution
        along the track of the dataset in meters.
    Parameters:
        download - Boolean to download raw .h5 files from NSIDC.
        data_type - Which dataset to download from NSIDC.
        spatial_extent - The extent for which to download ICESat-2 data from.
        date_range - Date range for download of ICESat-2 data.
        username - NASA EarthData username.
        email - NASA EarthData email.
        working_directory - Location for file storage.
        generate_csv - Process .h5 files with PhoREAL tool.
        track_num - Select which ATL03 tracks to derive canopy heights from,
            all by default.
        along_track_res - Resolution at which to derive canopy heights (meters).
        autocorrelation - Remove points located within a buffer of other points.
        autocorrelation_dist - Remove data within x meters of each other.

    TODO: Spatial autocorrelation?.
    TODO: Handle duplicate rows appropriately.
    TODO: Append dataframes to csv to handle memory?
        Sundarban 2019-01-01 to 2019-07-31, 117 csv files takes 15:48 to process.
    TODO: Improve canopy detection.
        Only select 95 percentile for canopy?
        DRAGANN Method?
    '''

    # Only execute code if input ATL03.h5 filepath and outpath declared
    if (working_directory):

        # Create download directory if necessary
        Path(working_directory).mkdir(parents=True, exist_ok=True)

        # Download ICESat-2 files if requested
        if download:
            # Create atl file directory
            h5_storage = working_directory + '/atl'
            Path(h5_storage).mkdir(parents=True, exist_ok=True)
            print('Downloading data from {} to {}.'.format(date_range[0],
                                                           date_range[1]))
            # Download specified ICESat-2 data
            download_icesat(
                data_type=data_type,
                spatial_extent=spatial_extent,
                date_range=date_range,
                username=username,
                email=email,
                output_location=h5_storage)
        else:
            h5_storage = working_directory
            print('Not downloading raw files from NSIDC.')
            pass

        # Create storage location for converted files
        output_location = working_directory + 'csv/'
        Path(output_location).mkdir(parents=True, exist_ok=True)

        # Process .h5 files with PhoREAL tool to derive stats
        if generate_csv:
            # Locate available .h5 files for processing
            atl03Files = glob.glob(os.path.join(h5_storage, f'*.h5'))
            # Print file names
            print('Located {} {} file(s).'.format(len(atl03Files), data_type))
            for files in tqdm_notebook(atl03Files):
                for track in track_num:
                    while True:
                        try:
                            getAtlMeasuredSwath(
                                atl03FilePath=files,
                                outFilePath=output_location,
                                gtNum=track,
                                trimInfo='auto',
                                createAtl03CsvFile=True)
                            break
                        except ValueError:
                            break
            print('Completed converting to .csv.')
        else:
            print('Not generating .csv files from .h5 files.')
            pass

        # Locate available csv files for analysis
        csvFiles = glob.glob(os.path.join(output_location, f'*.csv'))
        print(
            'Located {} .csv file(s) for canopy heights estimation.'.format(
                len(csvFiles)))

        # Confirm along-track resolution
        print('Canopy heights will be generated at a resolution of {} meters along-track.'.format(along_track_res))

        # Create empty 'merged' dataframe to concat other dataframes to
        merged_df = pd.read_csv(csvFiles[0], nrows=0)

        # Estimate ground and canopy returns from raw ATL03 data
        for csvs in tqdm_notebook(csvFiles):

            # Obtain .h5 file name
            #print('Processing: {}.'.format(csvs))

            # Load CSV file into dataframe
            df = pd.read_csv(csvs)

            # Sort out erroneous datasets by locating dataframes with 'NaN'
            # values.
            while True:
                try:
                    nbins = int(((df['Along-Track (m)'].max()) - \
                                (df['Along-Track (m)'].min())) / along_track_res)

                    # Filter to med. & high signal confidence for ground
                    # estimates
                    df_medhigh = df[df['Signal Confidence'] > 2]

                    # Calculate # of bins required for given resolution
                    nbins = int(((df_medhigh['Along-Track (m)'].max()) - (
                        df_medhigh['Along-Track (m)'].min())) / along_track_res)

                    # Sort dataframe in ascending order based on bins
                    df_medhigh.sort_values('Along-Track (m)', inplace=True)

                    # Create binned dataframe
                    df_ground = df_medhigh.copy()
                    df_ground['bin'] = pd.cut(df_medhigh['Along-Track (m)'],
                                              bins=nbins, include_lowest=True)

                    # Located lowest elevation of each bin to define as the
                    # 'ground'
                    df_ground['ground_est'] = df_ground.groupby(
                        'bin')['Height (m MSL)'].transform('min')
                    #print('Ground returns estimated for {}.'.format(csvs))

                    # Filter data to x classes for canopy estimations
                    df_canopy = df[df['Signal Confidence'] > 0]

                    # Create binned canopy dataframe
                    df_canopy['canopy_bin'] = pd.cut(
                        df_canopy['Along-Track (m)'], bins=nbins, include_lowest=True)

                    # Locate and define 95 Percentile return height for each
                    # bin
                    df_canopy['toc_est'] = df_canopy.groupby(
                        'canopy_bin')['Height (m MSL)'].transform('max')
                    df_canopy['toc_est'] = df_canopy['toc_est'].mul(0.95)

                    # Remove rows with no canopy height estimations
                    df_canopy = df_canopy.dropna(subset=['toc_est'])

                    # Remove canopy estimates that fall below ground level
                    df_canopy['elev_bin'] = pd.cut(
                        df_canopy['Along-Track (m)'], bins=nbins, include_lowest=True)

                    df_canopy['ground_est'] = df_canopy.groupby(
                        'elev_bin')['Height (m MSL)'].transform('min')

                    # Aggregate columns by bin and mean value
                    d_mean = df_canopy.groupby('canopy_bin').agg(['mean'])
                    # Flatten columns
                    d_mean.columns = d_mean.columns.map('_'.join)
                    # Revert back to proper dataframe
                    d_mean.reset_index()

                    # Calculate height of canopy above ground
                    d_mean['canopy_height_est'] = (d_mean['toc_est_mean'] -
                                                   d_mean['ground_est_mean'])
                    #print('Canopy returns estimated for {}.'.format(csvs))

                    # Remove rows with no coordinates
                    d_mean.dropna(
                        subset=['Longitude (deg)_mean'], inplace=True)

                    # Add track and year columns
                    year = csvs[-39:-35]
                    trackID = csvs[-8:-4]
                    d_mean['year'] = year
                    d_mean['track'] = trackID

                    # Concat final dataframe to merged set
                    merged_df = pd.concat([merged_df, d_mean])
                    print(
                        '{} canopy heights calculated...'.format(
                            len(merged_df)))
                    break

                except ValueError:
                    print('Erroneous data, skipping.')
                    break

        # Plot overview of created data
        # print(merged_df.head())
        #plot_overview(data=merged_df, resolution=along_track_res, save_path=output_location)

        # Drop empty/unnecessary columns
        merged_df.drop(merged_df.iloc[:, 0:22], axis=1, inplace=True)
        merged_df.drop(merged_df.iloc[:, 3:15], axis=1, inplace=True)

        # Rename Lat & Lon columns for GEE asset ingestion
        merged_df.rename(
            columns={
                'Latitude (deg)_mean': 'latitude',
                'Longitude (deg)_mean': 'longitude'},
            inplace=True)

        # Create storage directory for derived heights if necessary
        csvPath = output_location + '/canopy_estimates/'
        Path(csvPath).mkdir(parents=True, exist_ok=True)

        # Drop duplicate rows
        merged_df.drop_duplicates()

        # Save merged dataframe as csv
        merged_df.to_csv(csvPath + 'canopy_merged.csv', sep=',', index=False)
        print('Derived and saved merged canopy heights.')

        '''WORK IN PROGRESS'''
        # Handle spatial autocorrelation
        if autocorrelation:
            print('Handling autocorrelation.')
        else:
            print('Not handling autocorrelation. Processing complete.')

    else:
        print('ERROR: No .h5 files found and/or no output directory specified.')


'''WORK IN PROGRESS
Plot overview of processed data.
'''


def plot_overview(data=False, resolution=False, save_path=False):
    # Plot ground elevation
    plt.plot(
        data['Along-Track (m)'],
        data['ground_est_mean'],
        color='r',
        lw=0.75,
        zorder=1,
        label='Ground Level Est.')
    # Plot top of canopy elevation
    plt.plot(
        data['Along-Track (m)'],
        data['toc_est_mean'],
        color='g',
        lw=0.75,
        zorder=1,
        label='T.O.C. Est.')
    # Plot ground points
    plt.scatter(
        data['Along-Track (m)'],
        data['ground_est_mean'],
        color='r',
        s=2,
        alpha=0.6,
        zorder=3,
        label='Lowest Bin Elev.')
    # Plot canopy points
    plt.scatter(
        data['Along-Track (m)'],
        data['Height (m MSL)_mean'],
        color='black',
        s=0.25,
        alpha=0.6,
        zorder=2,
        label='Photon Returns')
    # Title
    plt.title(
        'Estimated T.O.C. Elevation ({}m Bins)'.format(resolution),
        fontsize=8)
    # X & Y Labels
    plt.xlabel('Along-Track (m)', fontsize=6)
    plt.ylabel('Return Height (m MSL)', fontsize=6)
    # Legend
    plt.legend(loc='upper right', prop={'size': 6})
    # Save generate figure
    plt.savefig(save_path + 'data_overview.png')
    print('Overview plot generated...')


'''WORK IN PROGRESS'''


def gmw_mangroves(
        gmw2016_path=False,
        spatial_extent=False,
        study_area_name=False,
        central_coord=False,
        buf_dist=10000,
        fix_geom=True):
    '''
    Description:
        Tool to clip GMW 2016 dataset to area of interest and generate files for GEE
        processing. GMW_2016 .shp files can be found here:
        https://data.unep-wcmc.org/datasets/45. Official shapefiles will be
        downloaded if none are found.
        This tool generates a shapefile with the following properties:
            1. Areas of known mangroves (as per GMW) within the designated aoi.
            2. Areas that are not mangroves within the designated aoi.
    Parameters:
        gmw2016_path - Directory with GMW 2016 shapefile.
        spatial_extent - Shapefile containing bbox of study area.
        study_area_name - Name of study area for storage purposes.
        central_coord = Lat/Long of center of study area to build buffer.
        buf_dist = Size of buffer, default 10000 meters (10km).

    TODO: Create bbox from upper left & upper right coords?
    TODO: Set 'pxlval' attribute of bbox == 0
    TODO: Fix GMW_2016 geometries
    TODO: Clip GMW_2016 to bbox
    TODO: Union bbox and clipped GMW_2016 together
    TODO: Simplify geometry
    TODO: Check if study_area_name is valid (no spaces, etc.)
    '''

    if (gmw2016_path and central_coord):
        # Locate available .shp files for processing
        gmw_polygons = glob.glob(os.path.join(
            f'gmw/GMW_001_GlobalMangroveWatch_2016/01_Data\\*GMW_2016_v2.shp'))

        # Obtain directory for file usage and storage
        directory = os.getcwd()

        # Download & unzip shapefiles if none are found
        while True:
            if len(gmw_polygons) == 0:
                url = 'https://wcmc.io/GMW_2016'
                file = 'GMW_001_GlobalMangroveWatch_2016.zip'
                print('No GMW shapefiles found. Downloading 2016 shapefiles...')

                # Download GMW 2016 shapefiles
                urllib.request.urlretrieve(url, gmw2016_path + file)

                # Unzip compressed files
                with zipfile.ZipFile(gmw2016_path + file, 'r') as zip_ref:
                    zip_ref.extractall(gmw2016_path)

                # Set file path to new location
                gmw2016_path = gmw2016_path + '/GMW_001_GlobalMangroveWatch_2016/01_Data'

                # Locate newly downloaded files
                gmw_polygons = glob.glob(os.path.join(gmw2016_path,
                                                      f'*GMW_2016_v2.shp'))

                # Load shapefile into geopandas dataframe
                gmw_polygons = gpd.read_file(gmw_polygons[0])
                print('Downloaded GMW 2016 shapefiles that can be found at: {}'.
                      format(gmw2016_path))
                break
            else:
                print('Located mangroves shapefile. Loading into geodataframe now...')
                # Load shapefile into geopandas dataframe
                gmw_polygons = gpd.read_file(directory + '/' + gmw_polygons[0])
                print('Loaded mangroves shapefile.')
                break

        # Build square buffer around central point of no aoi shapefile
        # provided.
        if not spatial_extent:
            print(
                'No aoi file provided, building aoi shapefile {} meters around {}.'.format(
                    buf_dist, central_coord))

            # Create dataframe for geopandas ingestion
            df = pd.DataFrame({'Latitude': [central_coord[0]],
                               'Longitude': [central_coord[1]]})

            # Load coordinates into geodataframe
            gdf = gpd.GeoDataFrame(
                df, geometry=gpd.points_from_xy(
                    df.Longitude, df.Latitude))

            # Buffer the coordinate
            # Note join_style: round = 1, mitre = 2, bevel = 3
            # Note cap_style: round = 1, flat = 2, square = 3
            gdf = gdf.buffer(buf_dist, join_style=2, cap_style=3)

            # Ask user for name of study area for storage location/creation
            folder_name = input('Name of study area for folder creation...')
            aoi_path = directory + '/' + folder_name + '/shapefiles/'
            Path(aoi_path).mkdir(parents=True, exist_ok=True)

            # Save to file for future use
            gdf.to_file(aoi_path + 'aoi.shp')
            print('AOI shapefile created and saved.')
        else:
            # Read AOI shapefile
            gdf = gpd.read_file(spatial_extent)
            print('Loaded provided aoi shapefile.')

        # Assign 'pxlval' to 0 for GEE use
        #gdf['pxlval'] = 0

        # Fix invalid GMW geometry is present
        if fix_geom:
            print('Fixing GMW geometry...')
            gmw_polygons = gmw_polygons.buffer(0)
            # Save to file for future use.
            gmw_polygons.to_file(gmw2016_path + 'gmw_fixed_geom.shp')
        else:
            print('Not fixing GMW geometry.')
            pass

        # Clip GMW shapefiles by aoi area
        gdf_clipped = gmw_polygons.clip(gdf)
        print('Clipped gdf...')
        print(gdf_clipped.head())

        '''
        # Define special characters for checking storage location
        special_characters = '"[@_!#$%^&*()<>?/\\|}{~:]"'

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
        print('ERROR: No .shp files found and/or incorrect directory specified.')


def icesat_autocorrelation(csv_path=False, buffer_dist=10):
    '''
    Description:
        Insert here.
    Parameters:
        Insert here

    TODO: Build tool
    '''
    if csv_path:
        # Insert calculations.
        print('Buffering points at a distance of {} meters.'.format(buffer_dist))
    else:
        print('Done.')
