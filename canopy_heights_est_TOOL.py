import pandas as pd
from pathlib import Path
from tqdm.notebook import tqdm_notebook
import os
import glob
import sys

from pandas.core.reshape.reshape import stack_multiple

# Import PhoREAL 'getAtlMeasuredSwath' tool
sys.path.insert(1, 'C:/Users/albp/mangroves/icesat/PhoREAL-master/source_code')
import getAtlMeasuredSwath_auto

def get_canopy_heights(atl03FileLocation = False, storePath = False, trackNum = 'gt1r', alongTrackRes = 10):
    
    # Only execute code if input ATL03.hf file and outpath declared
    if (atl03FileLocation and storePath):

        # Create storage directy if necessary
        Path(storePath).mkdir(parents=True, exist_ok=True)

        # Find available .h5 files for processing
        atl03Files = glob.glob(os.path.join(atl03FileLocation, f'*.h5'))
        # Print file names
        print('Found {} ATL03 files.'.format(len(atl03Files)))

        for files in tqdm_notebook(atl03Files):
            getAtlMeasuredSwath_auto.getAtlMeasuredSwath(atl03FilePath = files, outFilePath = storePath, gtNum = trackNum, trimInfo = 'auto', createAtl03CsvFile = True)
        
        print('Finished converting to csv.')

        # Find available csv files for analysis
        csvFiles = glob.glob(os.path.join(storePath, f'*.csv'))
        print('Located {} csv files for canopy heights estimation.'.format(len(csvFiles)))

        # Create storage directory for heights if necessary
        csvPath = storePath+'canopy_estimates/'
        Path(csvPath).mkdir(parents=True, exist_ok=True)

        # Confirm along-track resolution
        print('Canopy heights will be generated at a resolution of {} meters along-track.'.format(alongTrackRes))

        # Estimate ground and canopy returns from raw ATL03 data
        for csvs in tqdm_notebook(csvFiles):

            print('Processing: {}.'.format(csvs))
            file_name = csvs[-55:-4]

            # Load CSV file into dataframe
            df = pd.read_csv(csvs)

            ## TODO: Need to sort out errorneous datasets.
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