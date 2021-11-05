# -*- coding: utf-8 -*-
"""
Script to perform most basic functionalities required to get ATL03 swath

Copyright 2019 Applied Research Laboratories, University of Texas at Austin

This package is free software; the copyright holder gives unlimited
permission to copy and/or distribute, with or without modification, as
long as this notice is preserved.

Authors:
    Mike Alonzo
    Eric Guenther
    
Date: September 20, 2019
"""

# Import modules
import os
import numpy as np
import time as runTime
from icesatIO import (readAtl03H5, readAtl08H5,
                      readAtl03DataMapping, readAtl08DataMapping,
                      readTruthRegionsTxtFile,
                      writeLas, writeKml, writeArrayToCSV, writeLog,
                      GtToBeamNum, GtToBeamSW,
                      atlRotationStruct, atl03Struct, atl08Struct)
from icesatUtils import (getNameParts, getAtl08Mapping, getLatLon2UTM, 
                         getCoordRotFwd, getClosest, interp_vals)

     
# Function to read ICESat-2 data
def getAtlMeasuredSwath(atl03FilePath = False, atl08FilePath = False, 
                        outFilePath = False, gtNum = 'gt1r', trimInfo = 'auto', 
                        createAtl03LasFile = False, createAtl03KmlFile = False, 
                        createAtl08KmlFile = False, createAtl03CsvFile = False, 
                        createAtl08CsvFile = False, logFileID = False):
                        # pklHeaderFile = None, LAS_DIR = None):
    
    # Initialize outputs
    atl03Data = []
    headerData = False
    rotationData = False
    
    # Initialize ATL08 data
    atl08Data = []
    atl08_lat = []
    atl08_lon = []
    atl08_maxCanopy = []
    atl08_teBestFit = []
    atl08_teMedian = []
    atl08_time = []
    atl08_easting = []
    atl08_northing = []
    atl08_crossTrack = []
    atl08_alongTrack = []
    atl08_classification = []
    atl08_intensity = []

    # pklHeader = False
    # if type(pklHeaderFile) != type(None):
    #     pklHeader = True
    #     if type(LAS_DIR) == type(None):
    #         raise ValueError('LAS_DIR == None, please input LAS_DIR argument')
    
    # Only execute code if input ATL03 .h5 file and output path declared
    if(atl03FilePath and outFilePath):
    
        # Start timer
        timeStart = runTime.time()
        
        # Get beam # and S/W
        try:
            beamNum = GtToBeamNum(atl03FilePath, gtNum)
            beamStrength = GtToBeamSW(atl03FilePath, gtNum)
        except:
            beamNum = np.NaN
            beamStrength = np.NaN
        # endTry
                
        # Print message   
        writeLog('   Ground Track Number: %s (Beam #%s, Beam Strength: %s)\n' %(gtNum, beamNum, beamStrength), logFileID)
        
        # Get ATL03 file path/name
        atl03FilePath = os.path.normpath(os.path.abspath(atl03FilePath))
        atl03FileName = os.path.splitext(os.path.basename(atl03FilePath))[0]
        
        # Read ATL03 data from h5 file
        writeLog('   Reading ATL03 .h5 file: %s' % atl03FilePath, logFileID)
        lat_all = readAtl03H5(atl03FilePath, '/heights/lat_ph', gtNum)
        lon_all = readAtl03H5(atl03FilePath, '/heights/lon_ph', gtNum)
        z_all = readAtl03H5(atl03FilePath, '/heights/h_ph', gtNum)
        deltaTime_all = readAtl03H5(atl03FilePath, '/heights/delta_time', gtNum)
        signalConf_all = readAtl03H5(atl03FilePath, '/heights/signal_conf_ph', gtNum)
        zGeoidal = readAtl03H5(atl03FilePath, '/geophys_corr/geoid', gtNum)
        zGeoidal_deltaTime = readAtl03H5(atl03FilePath, '/geophys_corr/delta_time', gtNum)
        solar_elev = readAtl03H5(atl03FilePath, '/geolocation/solar_elevation', gtNum)
        solar_time = readAtl03H5(atl03FilePath, '/geolocation/delta_time', gtNum)
        atl03_ph_index_beg, atl03_segment_id, atl03_seg_deltaTime = readAtl03DataMapping(atl03FilePath, gtNum, return_delta_time=True)
        try:
            zGeoidal_all = interp_vals(zGeoidal_deltaTime, zGeoidal, deltaTime_all, removeThresh=True)
            zMsl_all = z_all - zGeoidal_all
            atl03_segment_id_interp = interp_vals(atl03_seg_deltaTime, atl03_segment_id, deltaTime_all)
            atl03_segment_id_interp = np.round(atl03_segment_id_interp)
            solar_elev_all = interp_vals(solar_time, solar_elev, deltaTime_all, removeThresh=True)
        except:
            zGeoidal_all = []            
            zMsl_all = np.empty(np.shape(z_all))
            zMsl_all[:] = np.NaN
            atl03_segment_id_interp = []
            solar_elev_all = []
        # endTry
            
        badVars = []
        if(len(lat_all)==0):
            badVar = 'Latitude (lat_ph)'
            badVars.append(badVar) 
        # endIf
        
        if(len(lon_all)==0):
            badVar = 'Longitude (lon_ph)'
            badVars.append(badVar) 
        # endIf
        
        if(len(z_all)==0):
            badVar = 'Height (h_ph)'
            badVars.append(badVar) 
        # endIf
        
        if(len(deltaTime_all)==0):
            badVar = 'Delta Time (delta_time)'
            badVars.append(badVar) 
        # endIf
        
        if(len(signalConf_all)==0):
            badVar = 'Signal Confidence (signal_conf_ph)'
            badVars.append(badVar) 
        # endIf
        
        if(len(badVars)==0):
        
            # Get time from delta time
            min_delta_time = np.min(deltaTime_all)
            time_all = deltaTime_all - min_delta_time
            
            # Get track direction
            if(np.abs(lat_all[-1]) >= np.abs(lat_all[0])):
                trackDirection = 'Ascending'
            else:
                trackDirection = 'Descending'
            # endIf
            
            writeLog('   Track Direction: %s' %trackDirection, logFileID)
            
            # Extract metadata from ATL03 file name
            atl03h5Info = getNameParts(atl03FileName)
            
            # Map ATL08 to ATL03 ground photons
            if(atl08FilePath):
            
                # Get ATL08 file path/name
                atl08FilePath = os.path.normpath(os.path.abspath(atl08FilePath))
                atl08FileName = os.path.splitext(os.path.basename(atl08FilePath))[0]
                atl08h5Info = getNameParts(atl08FileName)
                
                # Read ATL08 data from .h5 file
                writeLog('   Reading ATL08 .h5 file: %s' % atl08FilePath, logFileID) 
                atl08_lat = readAtl08H5(atl08FilePath, '/land_segments/latitude', gtNum)
                atl08_lon = readAtl08H5(atl08FilePath, '/land_segments/longitude', gtNum)
                atl08_maxCanopy = readAtl08H5(atl08FilePath, '/land_segments/canopy/h_max_canopy_abs', gtNum)
                atl08_teBestFit = readAtl08H5(atl08FilePath, '/land_segments/terrain/h_te_best_fit', gtNum)
                atl08_teMedian = readAtl08H5(atl08FilePath, '/land_segments/terrain/h_te_median', gtNum)
                atl08_deltaTime = readAtl08H5(atl08FilePath, '/land_segments/delta_time', gtNum)
                atl08_zGeoidal = interp_vals(zGeoidal_deltaTime, zGeoidal, atl08_deltaTime, removeThresh=True)
                atl08_maxCanopyMsl = atl08_maxCanopy - atl08_zGeoidal
                atl08_teBestFitMsl = atl08_teBestFit - atl08_zGeoidal
                atl08_teMedianMsl = atl08_teMedian - atl08_zGeoidal
                atl08_classed_pc_indx, atl08_classed_pc_flag, atl08_segment_id = readAtl08DataMapping(atl08FilePath, gtNum)
                atl08_signalConf = np.zeros(np.size(atl08_lat))
                atl08_classification = np.zeros(np.size(atl08_lat))
                atl08_intensity = np.zeros(np.size(atl08_lat))
                
                if((len(atl08_lat)>0) and (len(atl08_lon)>0)):
                    
                    # Get time from delta time
                    atl08_time = atl08_deltaTime - min_delta_time
                
                    # Do ATL08 to ATL03 mapping
                    writeLog('   Mapping ATL08 to ATL03 Ground Photons...', logFileID)
                    try:
                        classification_all = getAtl08Mapping(atl03_ph_index_beg, atl03_segment_id, atl08_classed_pc_indx, atl08_classed_pc_flag, atl08_segment_id)
                    except:
                        writeLog('   WARNING: Could not map ATL08 to ATL03 Ground Photons.', logFileID)
                        classification_all = atl08_classification
                    # endTry
                    
                    
                    # Get length to trim data by
                    class_length = len(classification_all)
                    lat_length = len(lat_all)
                    data_length = np.min([class_length, lat_length]) 
                    
                    # Trim ATL03 data down to size of classification array
                    atl03_lat = lat_all[0:data_length]
                    atl03_lon = lon_all[0:data_length]
                    atl03_z = z_all[0:data_length]
                    atl03_zMsl = zMsl_all[0:data_length]
                    atl03_time = time_all[0:data_length]
                    atl03_deltaTime = deltaTime_all[0:data_length]
                    atl03_signalConf = signalConf_all[0:data_length]
                    atl03_classification = classification_all[0:data_length]
                    atl03_intensity = np.zeros(np.size(atl03_lat))
                    atl03_solar_elev = solar_elev_all[0:data_length]
                    atl03_segment_id_interp = atl03_segment_id_interp[0:data_length]
                    dataIsMapped = True
                
                else:
                
                    writeLog('   WARNING: ATL08 file does not contain usable data.', logFileID)
                    
                    atl08FilePath = False
                    
                    # Store data
                    atl03_lat = lat_all
                    atl03_lon = lon_all
                    atl03_z = z_all
                    atl03_zMsl = zMsl_all
                    atl03_time = time_all
                    atl03_deltaTime = deltaTime_all
                    atl03_signalConf = signalConf_all
                    atl03_classification = np.zeros(np.size(atl03_lat))
                    atl03_intensity = np.zeros(np.size(atl03_lat))
                    atl03_solar_elev = solar_elev_all
                    atl03_segment_id_interp = atl03_segment_id_interp
                    dataIsMapped = False
                    
                # endIf
            
            else:
                
                # Message to screen
                writeLog('   Not Mapping ATL08 to ATL03 Ground Photons', logFileID)
                
                # Store data
                atl03_lat = lat_all
                atl03_lon = lon_all
                atl03_z = z_all
                atl03_zMsl = zMsl_all
                atl03_time = time_all
                atl03_deltaTime = deltaTime_all
                atl03_signalConf = signalConf_all
                atl03_classification = np.zeros(np.size(atl03_lat))
                atl03_intensity = np.zeros(np.size(atl03_lat))
                atl03_solar_elev = solar_elev_all
                atl03_segment_id_interp = atl03_segment_id_interp
                dataIsMapped = False
                
            # endIf
            
            # Get trim options
            trimParts = trimInfo.split(',')
            trimMode = trimParts[0]
            trimType = 'None'
            if(('manual' in trimMode.lower()) and (len(trimParts) > 1)):
                trimType = trimParts[1]
                trimMin = float(trimParts[2])
                trimMax = float(trimParts[3])
            # endIf
            
            
            # If selected to manually trim data, do this first
            if('manual' in trimMode.lower()):
                
                # Trim by lat or time
                if('lonlat' in trimType.lower()):
                    lonMin, lonMax = float(trimParts[2]), float(trimParts[3])
                    latMin, latMax = float(trimParts[4]), float(trimParts[5])
                    writeLog('   Manual Trim Mode (Min Lon: %s, Max Lon: %s)' % (lonMin, lonMax), logFileID)
                    writeLog('   Manual Trim Mode (Min Lat: %s, Max Lat: %s)' % (latMin, latMax), logFileID)
                    atl03IndsToKeepLon = (atl03_lon >= lonMin) & (atl03_lon <= lonMax)
                    atl03IndsToKeepLat = (atl03_lat >= latMin) & (atl03_lat <= latMax)
                    atl03IndsToKeep = atl03IndsToKeepLon & atl03IndsToKeepLat
                    
                    if(atl08FilePath):
                        atl08IndsToKeepLon = (atl08_lon >= lonMin) & (atl08_lon <= lonMax)
                        atl08IndsToKeepLat = (atl08_lat >= latMin) & (atl08_lat <= latMax)
                        atl08IndsToKeep = atl08IndsToKeepLon & atl08IndsToKeepLat


                elif('lat' in trimType.lower()):
                    writeLog('   Manual Trim Mode (Min Lat: %s, Max Lat: %s)' % (trimMin, trimMax), logFileID)
                    atl03IndsToKeep = (atl03_lat >= trimMin) & (atl03_lat <= trimMax)
                    
                    if(atl08FilePath):
                        atl08IndsToKeep = (atl08_lat >= trimMin) & (atl08_lat <= trimMax)
                    # endIf

                elif('lon' in trimType.lower()):
                    writeLog('   Manual Trim Mode (Min Lon: %s, Max Lon: %s)' % (trimMin, trimMax), logFileID)
                    atl03IndsToKeep = (atl03_lon >= trimMin) & (atl03_lon <= trimMax)
                    
                    if(atl08FilePath):
                        atl08IndsToKeep = (atl03_lon >= trimMin) & (atl03_lon <= trimMax)
                    # endIf

                    
                elif('time' in trimType.lower()):
                    writeLog('   Manual Trim Mode (Min Time: %s, Max Time: %s)' % (trimMin, trimMax), logFileID)
                    atl03IndsToKeep = (atl03_time >= trimMin) & (atl03_time <= trimMax)
                    
                    if(atl08FilePath):
                        atl08IndsToKeep = (atl08_time >= trimMin) & (atl08_time <= trimMax)
                    # endIf
                else:
                    writeLog('   Manual Trim Mode is Missing Args, Not Trimming Data', logFileID)
                    atl03IndsToKeep = np.ones(np.shape(atl03_lat), dtype = bool)
                    
                    if(atl08FilePath):
                        atl08IndsToKeep = np.ones(np.shape(atl08_lat), dtype = bool)
                    # endIf
                # endif
                
                # Trim ATL03 data
                if atl03IndsToKeep.sum() == 0:
                    # no data left given manual constraints
                    return atl03Data, atl08Data, headerData, rotationData
                # endIf

                atl03_lat = atl03_lat[atl03IndsToKeep]
                atl03_lon = atl03_lon[atl03IndsToKeep]
                atl03_z = atl03_z[atl03IndsToKeep]
                atl03_zMsl = atl03_zMsl[atl03IndsToKeep]
                atl03_time = atl03_time[atl03IndsToKeep]
                atl03_deltaTime = atl03_deltaTime[atl03IndsToKeep]
                atl03_signalConf = atl03_signalConf[atl03IndsToKeep]
                atl03_classification = atl03_classification[atl03IndsToKeep]
                atl03_intensity = atl03_intensity[atl03IndsToKeep]
                atl03_solar_elev = atl03_solar_elev[atl03IndsToKeep]
                atl03_segment_id_interp = atl03_segment_id_interp[atl03IndsToKeep]
                
                # Trim ATL08 data
                if(atl08FilePath):
                    atl08_lat = atl08_lat[atl08IndsToKeep]
                    atl08_lon = atl08_lon[atl08IndsToKeep]
                    atl08_maxCanopy = atl08_maxCanopy[atl08IndsToKeep]
                    atl08_teBestFit = atl08_teBestFit[atl08IndsToKeep]
                    atl08_teMedian = atl08_teMedian[atl08IndsToKeep]
                    atl08_maxCanopyMsl = atl08_maxCanopyMsl[atl08IndsToKeep]
                    atl08_teBestFitMsl = atl08_teBestFitMsl[atl08IndsToKeep]
                    atl08_teMedianMsl = atl08_teMedianMsl[atl08IndsToKeep]
                    atl08_time = atl08_time[atl08IndsToKeep]
                    atl08_deltaTime = atl08_deltaTime[atl08IndsToKeep]
                    atl08_signalConf = atl08_signalConf[atl08IndsToKeep]
                    atl08_classification = atl08_classification[atl08IndsToKeep]
                    atl08_intensity = atl08_intensity[atl08IndsToKeep]
                # endIf
            
            elif('none' in trimMode.lower()):
                
                writeLog('   Trim Mode: None', logFileID)
                
            # endIf
            
            # Remove ATL08 points above 1e30
            if(atl08FilePath):
                indsBelowThresh = (atl08_maxCanopy <= 1e30) | (atl08_teBestFit <= 1e30) | (atl08_teMedian <= 1e30)
                atl08_lat = atl08_lat[indsBelowThresh]
                atl08_lon = atl08_lon[indsBelowThresh]
                atl08_maxCanopy = atl08_maxCanopy[indsBelowThresh]
                atl08_teBestFit = atl08_teBestFit[indsBelowThresh]
                atl08_teMedian = atl08_teMedian[indsBelowThresh]
                atl08_maxCanopyMsl = atl08_maxCanopyMsl[indsBelowThresh]
                atl08_teBestFitMsl = atl08_teBestFitMsl[indsBelowThresh]
                atl08_teMedianMsl = atl08_teMedianMsl[indsBelowThresh]
                atl08_time = atl08_time[indsBelowThresh]
                atl08_deltaTime = atl08_deltaTime[indsBelowThresh]
                atl08_signalConf = atl08_signalConf[indsBelowThresh]
                atl08_classification = atl08_classification[indsBelowThresh]
                atl08_intensity = atl08_intensity[indsBelowThresh]
            # endIf
            
            # If selected to auto trim data, then trim if truth region exists
            if('auto' in trimMode.lower()):
                
                # Message to user
                writeLog('   Trim Mode: Auto', logFileID)
                
                # Read kmlBounds.txt file to get min/max extents to auto trim
                kmlBoundsTextFile = 'kmlBounds.txt'
                kmlRegionName = False
                if(os.path.exists(kmlBoundsTextFile)): # and not pklHeader:
                    
                    # Message to user
                    writeLog('   Finding Reference Region...', logFileID)
                    
                    try:
                        
                        # Read kmlBounds.txt file and get contents
                        kmlInfo = readTruthRegionsTxtFile(kmlBoundsTextFile)
    
                        # Loop through kmlBounds.txt and find matching TRUTH area
                        maxCounter = len(kmlInfo.regionName)
                        counter = 0
                        while(not kmlRegionName):
                            
                            latInFile = (atl03_lat >= kmlInfo.latMin[counter]) & (atl03_lat <= kmlInfo.latMax[counter])
                            lonInFile = (atl03_lon >= kmlInfo.lonMin[counter]) & (atl03_lon <= kmlInfo.lonMax[counter])
                            trackInRegion = any(latInFile & lonInFile)
                            
                            if(trackInRegion):
                                
                                # Get truth region info
                                kmlRegionName = kmlInfo.regionName[counter]
                                kmlLatMin = kmlInfo.latMin[counter]
                                kmlLatMax = kmlInfo.latMax[counter]
                                kmlLonMin = kmlInfo.lonMin[counter]
                                kmlLonMax = kmlInfo.lonMax[counter]
    
                                # Print truth region
                                writeLog('   Reference File Region: %s' % kmlRegionName, logFileID)
    
                            # endIf
                            
                            if(counter >= maxCounter):
                                
                                # Send message to user
                                writeLog('   No Reference File Region Found in kmlBounds.txt', logFileID)
                                break
                            
                            # endIf
                            
                            # Increment counter
                            counter += 1
                            
                        # endWhile
                    
                    except:
                        pass
                    # endTry
                # endIf
            
                if(kmlRegionName):
                    
                    # Trim ATL03 data based on TRUTH region
                    writeLog('   Auto-Trimming Data Based on Reference Region...', logFileID)
                    atl03IndsInRegion = (atl03_lat >= kmlLatMin) & (atl03_lat <= kmlLatMax) & (atl03_lon >= kmlLonMin) & (atl03_lon <= kmlLonMax)
                    atl03_lat = atl03_lat[atl03IndsInRegion]
                    atl03_lon = atl03_lon[atl03IndsInRegion]
                    atl03_z = atl03_z[atl03IndsInRegion]
                    atl03_zMsl = atl03_zMsl[atl03IndsInRegion]
                    atl03_time = atl03_time[atl03IndsInRegion]
                    atl03_deltaTime = atl03_deltaTime[atl03IndsInRegion]
                    atl03_signalConf = atl03_signalConf[atl03IndsInRegion]
                    atl03_classification = atl03_classification[atl03IndsInRegion]
                    atl03_intensity = atl03_intensity[atl03IndsInRegion]
                    atl03_solar_elev = atl03_solar_elev[atl03IndsInRegion]
                    atl03_segment_id_interp = atl03_segment_id_interp[atl03IndsInRegion]
                    
                    # Trim ATL08 data based on TRUTH region
                    if(atl08FilePath):
                        atl08IndsInRegion = (atl08_lat >= kmlLatMin) & (atl08_lat <= kmlLatMax) & (atl08_lon >= kmlLonMin) & (atl08_lon <= kmlLonMax)
                        atl08_lat = atl08_lat[atl08IndsInRegion]
                        atl08_lon = atl08_lon[atl08IndsInRegion]
                        atl08_maxCanopy = atl08_maxCanopy[atl08IndsInRegion]
                        atl08_teBestFit = atl08_teBestFit[atl08IndsInRegion]
                        atl08_teMedian = atl08_teMedian[atl08IndsInRegion]
                        atl08_maxCanopyMsl = atl08_maxCanopyMsl[atl08IndsInRegion]
                        atl08_teBestFitMsl = atl08_teBestFitMsl[atl08IndsInRegion]
                        atl08_teMedianMsl = atl08_teMedianMsl[atl08IndsInRegion]
                        atl08_time = atl08_time[atl08IndsInRegion]
                        atl08_deltaTime = atl08_deltaTime[atl08IndsInRegion]
                        atl08_signalConf = atl08_signalConf[atl08IndsInRegion]
                        atl08_classification = atl08_classification[atl08IndsInRegion]
                        atl08_intensity = atl08_intensity[atl08IndsInRegion]
                    #endIf
                # endIf
            # endIf
            
            # Convert lat/lon coordinates to UTM
            writeLog('   Converting Lat/Lon to UTM...', logFileID)
                    
            # Allow function to determine UTM zone for ATL03
            atl03_easting, atl03_northing, zone, hemi = getLatLon2UTM(atl03_lon, atl03_lat) 
            
            # Allow function to determine UTM zone for ATL03
            if(atl08FilePath):
                atl08_easting, atl08_northing, atl08_zone, atl08_hemi = getLatLon2UTM(atl08_lon, atl08_lat) 
            # endIf
            
            # Print UTM zone
            writeLog('   UTM Zone: %s %s' % (zone, hemi), logFileID)
          
            # Transform MEASURED data to CT/AT plane
            writeLog('   Computing CT/AT Frame Rotation...', logFileID)
            desiredAngle = 90
            atl03_crossTrack, atl03_alongTrack, R_mat, xRotPt, yRotPt, phi = getCoordRotFwd(atl03_easting, atl03_northing, [], [], [], desiredAngle)
            
            if(atl08FilePath):
                atl08_crossTrack, atl08_alongTrack, _, _, _, _ = getCoordRotFwd(atl08_easting, atl08_northing, R_mat, xRotPt, yRotPt, [])
            # endIf
            
            # Store rotation object
            rotationData = atlRotationStruct(R_mat, xRotPt, yRotPt, desiredAngle, phi)
            
            # Reassign class -1 to 0 (NOTE: this may need to change later)
            inds_eq_neg1 = atl03_classification == -1
            atl03_classification[inds_eq_neg1] = 0
                
            # Store data in class structure
            atl03Data = atl03Struct(atl03_lat, atl03_lon, \
                                    atl03_easting, atl03_northing, \
                                    atl03_crossTrack, atl03_alongTrack, \
                                    atl03_z, \
                                    atl03_zMsl,
                                    atl03_time, \
                                    atl03_deltaTime, \
                                    atl03_signalConf, atl03_classification, atl03_intensity, \
                                    atl03_solar_elev, \
                                    atl03_segment_id_interp, \
                                    gtNum, beamNum, beamStrength, zone, hemi, \
                                    atl03FilePath, atl03FileName, \
                                    trackDirection, \
                                    atl03h5Info, \
                                    dataIsMapped)
            
            # Store data in class structure
            if(atl08FilePath):
                atl08Data = atl08Struct(atl08_lat, atl08_lon, \
                                        atl08_easting, atl08_northing, \
                                        atl08_crossTrack, atl08_alongTrack, \
                                        atl08_maxCanopy, atl08_teBestFit, atl08_teMedian, \
                                        atl08_maxCanopyMsl, atl08_teBestFitMsl, atl08_teMedianMsl, \
                                        atl08_time, \
                                        atl08_deltaTime, \
                                        atl08_signalConf, atl08_classification, atl08_intensity, \
                                        gtNum, beamNum, beamStrength, atl08_zone, atl08_hemi, \
                                        atl08FilePath, atl08FileName, \
                                        trackDirection, \
                                        atl08h5Info, \
                                        dataIsMapped)
            # endIf
            
            # Create output ATL03 .las file
            if(createAtl03LasFile): 
                
                writeLog('   Writing ATL03 .las file...', logFileID)
                outName = atl03Data.atl03FileName + '_' + atl03Data.gtNum + '.las'
                outPath = os.path.normpath(outFilePath + '/' + outName)
                
                # If output directory does not exist, create it
                if(not os.path.exists(os.path.normpath(outFilePath))):
                    os.makedirs(os.path.normpath(outFilePath))
                # EndIf
                
                # Get projection
                if(atl03Data.zone=='3413' or atl03Data.zone=='3976'):
                    
                    # NSIDC Polar Stereographic North/South cases (3413 = Arctic, 3976 = Antarctic)
                    lasProjection = atl03Data.hemi
                    
                    # Write .las file
                    writeLas(np.ravel(atl03Data.easting),np.ravel(atl03Data.northing),np.ravel(atl03Data.z),lasProjection,outPath,np.ravel(atl03Data.classification),np.ravel(atl03Data.intensity),np.ravel(atl03Data.signalConf))
                    
                else:
                
                    # Write .las file for UTM projection case
                    writeLas(np.ravel(atl03Data.easting),np.ravel(atl03Data.northing),np.ravel(atl03Data.z),'utm',outPath,np.ravel(atl03Data.classification),np.ravel(atl03Data.intensity),np.ravel(atl03Data.signalConf),atl03Data.hemi,atl03Data.zone)
                
                # endIf
                
            # endIf
            
            # Create output ATL03 .kml file
            if(createAtl03KmlFile):
                
                writeLog('   Writing ATL03 .kml file...', logFileID)
                outName = atl03Data.atl03FileName + '_' + atl03Data.gtNum + '.kml'
                outPath = os.path.normpath(outFilePath + '/' + outName)
                
                # If output directory does not exist, create it
                if(not os.path.exists(os.path.normpath(outFilePath))):
                    os.makedirs(os.path.normpath(outFilePath))
                # EndIf
                
                # Get array of input time values
                timeStep = 1 # seconds
                timeVals = np.arange(np.min(atl03Data.time), np.max(atl03Data.time) + 1, timeStep)
            
                # Get closest time values from IceSat MEASURED data
                timeIn, indsToUse = getClosest(atl03Data.time, timeVals)
            
                # Reduce lat/lon values to user-specified time scale
                lonsIn = atl03Data.lon[indsToUse]
                latsIn = atl03Data.lat[indsToUse]
        
                # Write .kml file
                writeKml(latsIn, lonsIn, timeIn, outPath)
            
            # endIf
            
            # Create output ATL08 .kml file
            if(createAtl08KmlFile):
                
                if(atl08FilePath):
                
                    writeLog('   Writing ATL08 .kml file...', logFileID)
                    outName = atl08Data.atl08FileName + '_' + atl08Data.gtNum + '.kml'
                    outPath = os.path.normpath(outFilePath + '/' + outName)
                    
                    # If output directory does not exist, create it
                    if(not os.path.exists(os.path.normpath(outFilePath))):
                        os.makedirs(os.path.normpath(outFilePath))
                    # EndIf
                    
                    # Get array of input time values
                    timeStep = 1 # seconds
                    timeVals = np.arange(np.min(atl08Data.time), np.max(atl08Data.time) + 1, timeStep)
                
                    # Get closest time values from IceSat MEASURED data
                    timeIn, indsToUse = getClosest(atl08Data.time, timeVals)
                
                    # Reduce lat/lon values to user-specified time scale
                    lonsIn = atl08Data.lon[indsToUse]
                    latsIn = atl08Data.lat[indsToUse]
            
                    # Write .kml file
                    writeKml(latsIn, lonsIn, timeIn, outPath)
                
                # endIf
            
            # endIf
            
            # Create output ATL03 .csv file
            if(createAtl03CsvFile):
                
                writeLog('   Writing ATL03 .csv file...', logFileID)
                outName = atl03Data.atl03FileName + '_' + atl03Data.gtNum + '.csv'
                outPath = os.path.normpath(outFilePath + '/' + outName)
                
                # If output directory does not exist, create it
                if(not os.path.exists(os.path.normpath(outFilePath))):
                    os.makedirs(os.path.normpath(outFilePath))
                # EndIf
                
                # Create arrays for GT Num, Beam Num, and Beam Type
                gtNumArray = np.c_[np.tile(atl03Data.gtNum, len(atl03Data.lat))]
                beamNumArray = np.c_[np.tile(atl03Data.beamNum, len(atl03Data.lat))]
                beamTypeArray = np.c_[np.tile(atl03Data.beamStrength, len(atl03Data.lat))]
                zoneArray = np.c_[np.tile(atl03Data.zone, len(atl03Data.lat))]
                hemiArray = np.c_[np.tile(atl03Data.hemi, len(atl03Data.lat))]
                
                # Write .csv file
                if(atl03Data.zone=='3413' or atl03Data.zone=='3976'):
                    
                    namelist = ['Time (sec)', 'Delta Time (sec)', 'Segment ID', \
                                'GT Num', 'Beam Num', 'Beam Type', \
                                'Latitude (deg)', 'Longitude (deg)', \
                                'Polar Stereo X (m)', 'Polar Stereo Y (m)', \
                                'EPSG Code', 'Hemisphere', \
                                'Cross-Track (m)', 'Along-Track (m)', \
                                'Height (m HAE)', 'Height (m MSL)', \
                                'Classification', 'Signal Confidence', \
                                'Solar Elevation (deg)']
                else:
                    
                    namelist = ['Time (sec)', 'Delta Time (sec)', 'Segment ID', \
                                'GT Num', 'Beam Num', 'Beam Type', \
                                'Latitude (deg)', 'Longitude (deg)', \
                                'UTM Easting (m)', 'UTM Northing (m)', \
                                'UTM Zone', 'UTM Hemisphere', \
                                'Cross-Track (m)', 'Along-Track (m)', \
                                'Height (m HAE)', 'Height (m MSL)', \
                                'Classification', 'Signal Confidence', \
                                'Solar Elevation (deg)']
                # endIf
                
                datalist = [atl03Data.time, atl03Data.deltaTime, atl03Data.segmentID, \
                            gtNumArray, beamNumArray, beamTypeArray, \
                            atl03Data.lat, atl03Data.lon, \
                            atl03Data.easting, atl03Data.northing, \
                            zoneArray, hemiArray, \
                            atl03Data.crossTrack, atl03Data.alongTrack,\
                            atl03Data.z, atl03Data.zMsl, \
                            atl03Data.classification, atl03Data.signalConf, \
                            atl03Data.solar_elev] 
                
                writeArrayToCSV(outPath, namelist, datalist)
                
            # endIf
            
            # Create output ATL08 .csv file
            if(createAtl08CsvFile):
                
                if(atl08FilePath):
                    
                    writeLog('   Writing ATL08 .csv file...', logFileID)
                    outName = atl08Data.atl08FileName + '_' + atl08Data.gtNum + '.csv'
                    outPath = os.path.normpath(outFilePath + '/' + outName)
                    
                    # If output directory does not exist, create it
                    if(not os.path.exists(os.path.normpath(outFilePath))):
                        os.makedirs(os.path.normpath(outFilePath))
                    # EndIf
                    
                    # Create arrays for GT Num, Beam Num, and Beam Type
                    gtNumArray = np.c_[np.tile(atl08Data.gtNum, len(atl08Data.lat))]
                    beamNumArray = np.c_[np.tile(atl08Data.beamNum, len(atl08Data.lat))]
                    beamTypeArray = np.c_[np.tile(atl08Data.beamStrength, len(atl08Data.lat))]
                    zoneArray = np.c_[np.tile(atl08Data.zone, len(atl08Data.lat))]
                    hemiArray = np.c_[np.tile(atl08Data.hemi, len(atl08Data.lat))]
                    
                    # Write .csv file
                    if(atl03Data.zone=='3413' or atl03Data.zone=='3976'):
                        
                        namelist = ['Time (sec)', 'Delta Time (sec)', \
                                    'GT Num', 'Beam Num', 'Beam Type', \
                                    'Latitude (deg)', 'Longitude (deg)', \
                                    'Polar Stereo X (m)', 'Polar Stereo Y (m)', \
                                    'EPSG Code', 'Hemisphere', \
                                    'Cross-Track (m)', 'Along-Track (m)', \
                                    'Max Canopy (m)', \
                                    'Terrain Best Fit (m)', 'Terrain Median (m)']
                    else:
                        
                        namelist = ['Time (sec)', 'Delta Time (sec)', \
                                    'GT Num', 'Beam Num', 'Beam Type', \
                                    'Latitude (deg)', 'Longitude (deg)', \
                                    'UTM Easting (m)', 'UTM Northing (m)', \
                                    'UTM Zone', 'UTM Hemisphere', \
                                    'Cross-Track (m)', 'Along-Track (m)', \
                                    'Max Canopy (m)', \
                                    'Terrain Best Fit (m)', 'Terrain Median (m)']
                    # endIf
                
                    datalist = [atl08Data.time, atl08Data.deltaTime, \
                                gtNumArray, beamNumArray, beamTypeArray, \
                                atl08Data.lat, atl08Data.lon, \
                                atl08Data.easting, atl08Data.northing, \
                                zoneArray, hemiArray, \
                                atl08Data.crossTrack, atl08Data.alongTrack,\
                                atl08Data.maxCanopy, \
                                atl08Data.teBestFit, atl08Data.teMedian] 
                    
                    writeArrayToCSV(outPath, namelist, datalist)
                
                # endIf
                
            # endIf
            
            # End timer
            timeEnd = runTime.time()
            timeElapsedTotal = timeEnd - timeStart
            timeElapsedMin = np.floor(timeElapsedTotal / 60)
            timeElapsedSec = timeElapsedTotal % 60
            
            # Print completion message
            writeLog('', logFileID)
            writeLog('   Module Completed in %d min %d sec.' % (timeElapsedMin, timeElapsedSec), logFileID)
            writeLog('\n', logFileID)
            
        else:
        
            writeLog('\n', logFileID)
            writeLog('   *** Could not process data.', logFileID)
            writeLog('   *** ATL03 .h5 file missing these fields:', logFileID)
            for i in range(0,len(badVars)):
                writeLog('          %s) %s' %(i+1, badVars[i]), logFileID)
            writeLog('\n', logFileID)
        
        # endIf
            
    else:
            
        # Message to user
        writeLog('Input correct ATL03 input .h5 file and/or output path.', logFileID)
            
    # endIf
        
    # Return object
    return atl03Data, atl08Data, rotationData

    
# Unit test
if __name__ == "__main__":
    
    ##### Start Inputs for getAtlMeasuredSwath

    # Path to ATL03 Input File
    # atl03FilePath = '//lidar-server/lidar/USERS/eric/benjelly_atl08/ATL03_20181126114738_08990103_001_01.h5' # FINLAND
    # atl03FilePath = '//lidar-server/lidar/USERS/eric/benjelly_atl08/ATL03_20181030110205_04860106_001_01.h5' # SONONMA
    # atl03FilePath = '//lidar-server/lidar/USERS/eric/benjelly_atl08/ATL03_20190101195003_00670202_001_01_sreq_2696.h5' # SONOMA
    # atl03FilePath = '//lidar-server/lidar/USERS/eric/benjelly_atl08/ATL03_20190228170214_09510202_001_02_sreq_2696.h5' # SONOMA
    # atl03FilePath = '//bigtex/laserpewpew/data/release/001/ATL03_r001/ATL03_20190426213703_04370308_001_01.h5' # Brazil    
    # atl03FilePath = 'Z:/data/release/001/ATL03_r001/ATL03_20181017131058_02890103_001_01_sub_208.h5' # FINLAND BAD DATA
    atl03FilePath = 'Z:/data/release/R002/ATL03_rR002/ATL03_20190928175636_00280506_R002_01_sreq_3002.h5' # WSMR
    # atl03FilePath = 'Z:/data/release/001/ATL03_r001/ATL03_20181210094923_11110111_001_01_sub_283.h5' # 88 South
    
    # Path to ATL08 Input File
    # atl08FilePath = '//lidar-server/lidar/USERS/eric/benjelly_atl08/ATL08_20181126114738_08990103_952_01.h5' # FINLAND
    # atl08FilePath = '//lidar-server/lidar/USERS/eric/benjelly_atl08/ATL08_20181030110205_04860106_952_01.h5' # SONOMA
    # atl08FilePath = '//lidar-server/lidar/USERS/eric/benjelly_atl08/ATL08_20190101195003_00670202_952_01.h5' # SONOMA
    # atl08FilePath = '//lidar-server/lidar/USERS/eric/benjelly_atl08/ATL08_20190228170214_09510202_952_02.h5' # SONOMA
    # atl08FilePath = False    
    # atl08FilePath = 'Z:/data/release/001/ATL08_r001/ATL08_20181017131058_02890103_001_01_sub_208.h5' # FINLAND BAD DATA
    atl08FilePath = 'Z:/data/release/R002/ATL08_rR002/ATL08_20190928175636_00280506_R002_01_sub_344.h5' # WSMR
    
    # Path to Output Directory
    # outFilePath = '//lidar-server/lidar/USERS/mike/iceSat2/atl03_validation/r001_finland_20181126_python' # FINLAND
    # outFilePath = '//lidar-server/lidar/USERS/mike/iceSat2/atl03_validation/r001_sonoma_20181030_python' # SONOMA
    # outFilePath = '//lidar-server/lidar/USERS/mike/iceSat2/atl03_validation/r001_sonoma_20190101_python' # SONOMA
    # outFilePath = '//lidar-server/lidar/USERS/mike/iceSat2/atl03_validation/r001_sonoma_20190228_python' # SONOMA
    # outFilePath = '//lidar-server/lidar/USERS/mike/iceSat2/atl03_validation/test'
    outFilePath = 'C:/Users/malonzo/GLAM/delete'
    
    # Ground track number to analyze
    gtNum = 'gt2r'
    
#    if os.name == 'nt':
#        basepath03 = '/laserpewpew/data/release/002/ATL03_r002/Maine'
#    else:
#        basepath03 = '/laserpewpew/data/release/002/ATL03_r002/Maine/'
#        basepath08 = '/laserpewpew/data/release/002/ATL08_r002/Maine/'
#
#    atl03file = 'ATL03_20181114182133_07200102_002_01.h5'
#    atl08file = 'ATL08_20181114182133_07200102_002_01.h5'
#    atl09file = 'ATL09_20181016132105_02740101_002_01.h5'
#    # Inputs
#    
#    outFilePath = '/LIDAR/server/USERS/eric/1_experiment/test'
#
#    atl03FilePath = basepath03 + atl03file
#    atl08FilePath = basepath08 + atl08file
    
    # User options
    trimInfo = 'auto'   # OPTIONS: ('none', 'auto', or 'manual')
                        # none - does not trim data at all
                        # auto - trims ATL03 track to extent of truth bounding region
                        # manual - trims ATL03 track by latitude or time
                            # Example: 'manual,lat,38,39'
                            # Only uses data between latitude 38 and 39 deg
                            # Example: 'manual,time,3,4'
                            # Only uses data between time 3 and 4 seconds
    
    createAtl03LasFile = False    # Option to create output measured ATL03 .las file
    createAtl03KmlFile = False    # Option to create output measured ATL03 .kml file
    createAtl08KmlFile = False    # Option to create output measured ATL08 .kml file
    createAtl03CsvFile = False    # Option to create output measured ATL03 .csv file
    createAtl08CsvFile = False    # Option to create output measured ATL08 .csv file
 
    ##### End Inputs for getAtlMeasuredSwath
    
    
    ##### CODE BELOW -- DO NOT EDIT ###############################################
    
    timeStart = runTime.time()
    
    # Call getAtlMeasuredSwath
    writeLog('RUNNING getAtlMeasuredSwath...\n')
    atl03Data, atl08Data, rotationData = getAtlMeasuredSwath(atl03FilePath, 
                                                                         atl08FilePath, 
                                                                         outFilePath, 
                                                                         gtNum, 
                                                                         trimInfo, 
                                                                         createAtl03LasFile, 
                                                                         createAtl03KmlFile, 
                                                                         createAtl08KmlFile, 
                                                                         createAtl03CsvFile, 
                                                                         createAtl08CsvFile)
        
    # End timer
    timeEnd = runTime.time()
    timeElapsedTotal = timeEnd - timeStart
    timeElapsedMin = np.floor(timeElapsedTotal / 60)
    timeElapsedSec = timeElapsedTotal % 60
        
    # Print completion message
    writeLog('   Script Completed in %d min %d sec.' % (timeElapsedMin, timeElapsedSec))
    writeLog('\n')
    
#    import matplotlib.pyplot as plt

#    writeLog('Test AT')
#    hy = atl03Data.alongTrack[atl03Data.classification == 3]
#    hz = atl03Data.z[atl03Data.classification == 3]
#    cy = atl03Data.alongTrack[atl03Data.classification == 2]
#    cz = atl03Data.z[atl03Data.classification == 2]
#    gy = atl03Data.alongTrack[atl03Data.classification == 1]
#    gz = atl03Data.z[atl03Data.classification == 1]
#    dy = atl03Data.alongTrack[atl03Data.classification == 0]
#    dz = atl03Data.z[atl03Data.classification == 0]
#    uy = atl03Data.alongTrack[atl03Data.classification == -1]
#    uz = atl03Data.z[atl03Data.classification == -1]
#    
#    atl08y = atl08Data.alongTrack
#    atl08mcz = atl08Data.maxCanopy
#    atl08tbfz = atl08Data.teBestFit   
#    atl08tmz = atl08Data.teMedian 
#
#    atl08mcz[atl08mcz > 10000] = np.nan
#    atl08tbfz[atl08tbfz > 10000] = np.nan
#    atl08tmz[atl08tmz > 10000] = np.nan
#    
#    
#
#    
#    f1 = plt.plot()
#    f1 = plt.plot(uy[::100],uz[::100],'.',color='#d7d7d7')
#    f1 = plt.plot(dy[::10],dz[::10],'.',color='#8db4c1')
#    f1 = plt.plot(hy,hz,'.',color='#90f045')
#    f1 = plt.plot(cy,cz,'.',color='#4e7b2a')
#    f1 = plt.plot(gy,gz,'.',color='#d6a327')
#    
#    f1 = plt.plot(atl08y, atl08mcz,'o',color='#33cc33')
#    f1 = plt.plot(atl08y, atl08tbfz,'o',color='#0066ff')
#    f1 = plt.plot(atl08y, atl08tmz,'o',color='#ff3300')
#
#
#
#    writeLog('Test Time')
#    hy = atl03Data.time[atl03Data.classification == 3]
#    hz = atl03Data.z[atl03Data.classification == 3]
#    cy = atl03Data.time[atl03Data.classification == 2]
#    cz = atl03Data.z[atl03Data.classification == 2]
#    gy = atl03Data.time[atl03Data.classification == 1]
#    gz = atl03Data.z[atl03Data.classification == 1]
#    dy = atl03Data.time[atl03Data.classification == 0]
#    dz = atl03Data.z[atl03Data.classification == 0]
#    uy = atl03Data.time[atl03Data.classification == -1]
#    uz = atl03Data.z[atl03Data.classification == -1]
#    
#    atl08y = atl08Data.time
#    atl08mcz = atl08Data.maxCanopy
#    atl08tbfz = atl08Data.teBestFit   
#    atl08tmz = atl08Data.teMedian 
#
#    atl08mcz[atl08mcz > 10000] = np.nan
#    atl08tbfz[atl08tbfz > 10000] = np.nan
#    atl08tmz[atl08tmz > 10000] = np.nan
#    
#    
#
#    
#    f2 = plt.plot()
#    f2 = plt.plot(uy[::100],uz[::100],'.',color='#d7d7d7')
#    f2 = plt.plot(dy[::10],dz[::10],'.',color='#8db4c1')
#    f2 = plt.plot(hy,hz,'.',color='#90f045')
#    f2 = plt.plot(cy,cz,'.',color='#4e7b2a')
#    f2 = plt.plot(gy,gz,'.',color='#d6a327')
#    
#    f2 = plt.plot(atl08y, atl08mcz,'o',color='#ccff33')
#    f2 = plt.plot(atl08y, atl08tbfz,'o',color='#0066ff')
#    f2 = plt.plot(atl08y, atl08tmz,'o',color='#ff3300')    
    
    
    
    # print('Test UTM Northing')
    # hy = atl03Data.northing[atl03Data.classification == 3]
    # hz = atl03Data.z[atl03Data.classification == 3]
    # cy = atl03Data.northing[atl03Data.classification == 2]
    # cz = atl03Data.z[atl03Data.classification == 2]
    # gy = atl03Data.northing[atl03Data.classification == 1]
    # gz = atl03Data.z[atl03Data.classification == 1]
    # dy = atl03Data.northing[atl03Data.classification == 0]
    # dz = atl03Data.z[atl03Data.classification == 0]
    # uy = atl03Data.northing[atl03Data.classification == -1]
    # uz = atl03Data.z[atl03Data.classification == -1]
    
    # atl08y = atl08Data.northing
    # atl08mcz = atl08Data.maxCanopy
    # atl08tbfz = atl08Data.teBestFit   
    # atl08tmz = atl08Data.teMedian 

    # atl08mcz[atl08mcz > 10000] = np.nan
    # atl08tbfz[atl08tbfz > 10000] = np.nan
    # atl08tmz[atl08tmz > 10000] = np.nan
    
    

    
    # f3 = plt.plot()
    # f3 = plt.plot(uy[::100],uz[::100],'.',color='#d7d7d7')
    # f3 = plt.plot(dy[::10],dz[::10],'.',color='#8db4c1')
    # f3 = plt.plot(hy,hz,'.',color='#90f045')
    # f3 = plt.plot(cy,cz,'.',color='#4e7b2a')
    # f3 = plt.plot(gy,gz,'.',color='#d6a327')
    
    # f3 = plt.plot(atl08y, atl08mcz,'o',color='#ccff33')
    # f2 = plt.plot(atl08y, atl08tbfz,'o',color='#0066ff')
    # f23= plt.plot(atl08y, atl08tmz,'o',color='#ff3300')    
    

    
    
# endIf