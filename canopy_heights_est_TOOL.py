import glob
import os
import sys
from pathlib import Path

import icepyx as ipx
import pandas as pd
from pandas.core.reshape.reshape import stack_multiple
from tqdm.notebook import tqdm_notebook

# Import PhoREAL 'getAtlMeasuredSwath' tool
sys.path.insert(1, '../PhoREAL-master/source_code')
import getAtlMeasuredSwath_auto

def get_canopy_heights(download = False, data_type = 'ATL03', spatial_extent = False, date_range = False, username = False, email = False, atl03FileLocation = False, storePath = False, trackNum = 'gt2r', alongTrackRes = 10):
    
    # Only execute code if input ATL03.h5 filepath and outpath declared
    if (atl03FileLocation and storePath):

        # Create download directory if necessary
        Path(atl03FileLocation).mkdir(parents=True, exist_ok=True)

        # Create storage directory if necessary
        Path(storePath).mkdir(parents=True, exist_ok=True)

        # Download ICESat-2 files
        if download == True:
            region_a = ipx.Query(data_type, spatial_extent, date_range)
            region_a.earthdata_login(username, email)
            region_a.order_granules()
            region_a.granules.orderIDs
            region_a.download_granules(atl03FileLocation)
        else:
            print('Not downloading raw files from NSIDC.')
            pass

        # Find available .h5 files for processing
        atl03Files = glob.glob(os.path.join(atl03FileLocation, f'*.h5'))
        # Print file names
        print('Found {} ATL03 file(s).'.format(len(atl03Files)))

        # Process .h5 files with PhoREAL tool to convert to csv with return statistics
        for files in tqdm_notebook(atl03Files):
            getAtlMeasuredSwath_auto.getAtlMeasuredSwath(atl03FilePath = files, outFilePath = storePath, gtNum = trackNum, trimInfo = 'auto', createAtl03CsvFile = True)
        
        print('Finished converting to csv.')

        # Find available csv files for analysis
        csvFiles = glob.glob(os.path.join(storePath, f'*.csv'))
        print('Located {} csv file(s) for canopy heights estimation.'.format(len(csvFiles)))

        # Create storage directory for heights if necessary
        csvPath = storePath+'/CANOPY_ESTIMATES/'
        Path(csvPath).mkdir(parents=True, exist_ok=True)

        # Confirm along-track resolution
        print('Canopy heights will be generated at a resolution of {} meters along-track.'.format(alongTrackRes))

        # Estimate ground and canopy returns from raw ATL03 data
        for csvs in tqdm_notebook(csvFiles):

            # Obtain .h5 file name
            print('Processing: {}.'.format(csvs))
            file_name = csvs[-55:-4]

            # Load CSV file into dataframe
            df = pd.read_csv(csvs)

            # Sort out errorneous datasets. Done by locating dataframes with 'NaN' values.
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
                    # Group by bins and calculate mean value of each column for each bin
                    d_mean = df_canopy.groupby('canopy_bin').agg(['mean'])
                    # Flatten columns
                    d_mean.columns = d_mean.columns.map('_'.join)
                    # Revert back to proper dataframe
                    d_mean.reset_index()

                    # Calculate height of canopy above ground
                    d_mean['canopy_height_est'] = d_mean['toc_est_mean'] - d_mean['ground_est_mean']

                    # Remove rows with no coordinates
                    d_mean.dropna(subset=['Longitude (deg)_mean'], inplace=True)

                    # Save results to csv
                    d_mean.to_csv(csvPath+file_name+'_canopy.csv', sep=',')
                    print('Derived canopy heights and saved {} to csv.'.format(csvs))
                    break
                
                except ValueError:
                    print('Erroneous data, skipping.')
                    break

    else:
        print('ERROR: No .h5 files found in specified location and/or incorrect output directory specified.')
