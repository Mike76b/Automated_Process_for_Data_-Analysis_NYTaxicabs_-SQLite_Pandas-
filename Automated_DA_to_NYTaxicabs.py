# -*- coding: utf-8 -*-
"""
Created on Wed Oct 24 13:13:29 2018

@author: Mike
"""

# Import the modules-libraries

import pandas as pd
import matplotlib.pyplot as plt
import sqlite3 
import os
import operator
import datetime as dt


# Enter the path and move to it

workingPath = "D:/ProgramData/Data Resources/NYTaxi/NYTaxi_2016"
os.chdir(workingPath)

# Creating the destination folder in which the produced files will be placed at the end

try:
    os.makedirs("D:/ProgramData/Data Resources/NYTaxi/NYTaxi_2016/ProducedFiles")
    destinationFolder = "D:/ProgramData/Data Resources/NYTaxi/NYTaxi_2016/ProducedFiles"

except:
    destinationFolder = "D:/ProgramData/Data Resources/NYTaxi/NYTaxi_2016/ProducedFiles"

# Read the .csv files inside the folder and create a list of them

workingFilesList = [dataFile for dataFile in os.listdir("{}".format(workingPath)) 
                    if dataFile.endswith(".csv")]


# Get the file to work on it

for file in workingFilesList:
    
    # Create a .txt report-file in which will be printed progress messages

    with open("Report_{}.txt".format(file[15:22]), "a") as reportFile:
        print("Report of {} file.".format(file), file=reportFile)
        print("Processing file started at: {}".format(dt.datetime.today().strftime("%Y-%m-%d %H:%M:%S")),
              file=reportFile)
        print("\n", file=reportFile)
        
    # Processing the file
    
        # Reading the .csv file
        currentDf = pd.read_csv(file, sep=',')
        print("Initial shape of data:", currentDf.shape, file=reportFile)
        
        # Getting the columns of the original data
        cols = currentDf.columns
        print("Original columns list:", cols, file=reportFile)
        print("\n", file=reportFile)
        
        # Creating a reduced DF by dropping the columns that won't be needed
        rCurrentDf = pd.DataFrame(currentDf, columns=['lpep_pickup_datetime', 
                                                      'Pickup_longitude', 'Pickup_latitude'])
        print("DataFrame was reduced to the columns that will be used. The top three rows are:", 
              file=reportFile)
        print(rCurrentDf.head(3), file=reportFile)
        print("\n", file=reportFile)
        
        # Looking for null values
        rowsBefore = rCurrentDf.shape[0]
        for column in rCurrentDf:
            print("Is in {} any null value?".format(column), rCurrentDf[column].isnull().any(), 
                  file=reportFile)
            rCurrentDf.dropna()
            rowsAfter = rCurrentDf.shape[0]
            print("Total rows deleted:", rowsBefore - rowsAfter, file=reportFile)
        print("\n", file=reportFile)
        
        # Getting range of values (other data quality review)
        for eachColumn in rCurrentDf:
            print("Original Max value in {} is {}, and Min value is {}".format(eachColumn, 
                  rCurrentDf[eachColumn].max(), rCurrentDf[eachColumn].min()), file=reportFile)
        print("Current number of rows in dataset:", rCurrentDf.shape[0], file=reportFile)
        print("\n", file=reportFile)
        
        # Removing previous outliers
        colsToUse = ['lpep_pickup_datetime', 'Pickup_longitude', 'Pickup_latitude']

        secondColumnFiltering1 = rCurrentDf[colsToUse[1]] <= -73
        rCurrentDf = rCurrentDf[secondColumnFiltering1]
        
        secondColumnFiltering2 = rCurrentDf[colsToUse[1]] >= -75
        rCurrentDf = rCurrentDf[secondColumnFiltering2]
        
        thirdColumnFiltering1 = rCurrentDf[colsToUse[2]] <= 41
        rCurrentDf = rCurrentDf[thirdColumnFiltering1]
        
        thirdColumnFiltering2 = rCurrentDf[colsToUse[2]] >= 40
        rCurrentDf = rCurrentDf[thirdColumnFiltering2]
        
        for eachColumn in rCurrentDf:
            print("After cleaning, Max value in {} is {}, and min value is {}".format(eachColumn, 
                  rCurrentDf[eachColumn].max(), rCurrentDf[eachColumn].min()), file=reportFile)
        print("Current number of rows in dataset:", rCurrentDf.shape[0], file=reportFile)
        print("\n", file=reportFile)
        rowsAfter = rCurrentDf.shape[0]
        print("Total rows deleted:", rowsBefore - rowsAfter, file=reportFile)
        print("\n", file=reportFile)
        print("\n", file=reportFile)
        
        # Spliting the first column in date and time
        rCurrentDf['Pickup_Date'], rCurrentDf['Pickup_Time'] = rCurrentDf['lpep_pickup_datetime'].str.split(' ').str
        rCurrentDf = rCurrentDf.drop('lpep_pickup_datetime', 1)
        rCurrentDf = rCurrentDf[['Pickup_Date', 'Pickup_Time', 'Pickup_longitude', 'Pickup_latitude']]
        print("First column has been successfully splitted, now columns are:", file=reportFile)
        print(rCurrentDf.columns, file=reportFile)
        print("\n", file=reportFile)
        
        # Creating four subsets from the DataFrame to be plotted and flagged 
        nightDf = pd.DataFrame(rCurrentDf.loc[operator.or_(rCurrentDf['Pickup_Time'] >= '23:00:01', 
                                                           rCurrentDf['Pickup_Time'] <= '05:00:00')])

        morningDf = pd.DataFrame(rCurrentDf.loc[operator.and_(rCurrentDf['Pickup_Time'] >= '05:00:01', 
                                                              rCurrentDf['Pickup_Time'] <= '11:00:00')])
        
        middayDf = pd.DataFrame(rCurrentDf.loc[operator.and_(rCurrentDf['Pickup_Time'] >= '11:00:01', 
                                                             rCurrentDf['Pickup_Time'] <= '17:00:00')])
        
        afternoonDf = pd.DataFrame(rCurrentDf.loc[operator.and_(rCurrentDf['Pickup_Time'] >= '17:00:01', 
                                                                rCurrentDf['Pickup_Time'] <= '23:00:00')])
        print("Main DataFrame and the four subsets have the same length:",
              len(rCurrentDf)==(len(nightDf) + len(morningDf) + len(middayDf) + len(afternoonDf)),"\n",
              "Difference is:", len(rCurrentDf) - (len(nightDf) + len(morningDf) + len(middayDf) + len(afternoonDf)),
              file=reportFile)
        print("\n", file=reportFile)
        
        nightDf['Horary']="Night"
        morningDf['Horary']="Morning"
        middayDf['Horary']="Midday"
        afternoonDf['Horary']="Afternoon"
        print("First five rows of nightDf are: \n", nightDf.head(), "\n", file=reportFile)
        print("First five rows of morningDf are: \n", morningDf.head(), "\n", file=reportFile)
        print("First five rows of middayDf are: \n", middayDf.head(), "\n", file=reportFile)
        print("First five rows of afternoonDf are: \n", afternoonDf.head(), "\n", file=reportFile)
        
        # New DataFrame with all the subsets
        allDayDF = pd.concat([nightDf, morningDf, middayDf, afternoonDf])
        
        # Create a .csv file with the cleaned data
        allDayDF.to_csv("{}_allDayDF".format(rCurrentDf['Pickup_Date'][1][0:7]))
        print("The file {}_allDayDF.csv was successfully created".format(rCurrentDf['Pickup_Date'][1][0:7]),
              file=reportFile)
        print("\n", file=reportFile)
        
        # Create a .sqlite3 file with the cleaned data
        conn = sqlite3.connect("{}_Data.sqlite3".format(rCurrentDf['Pickup_Date'][1][0:7]))
        c = conn.cursor()
        
        nightDf.to_sql(name="Night_Table", 
                       con=sqlite3.connect("{}_Data.sqlite3".format(rCurrentDf['Pickup_Date'][1][0:7])))
        morningDf.to_sql(name="Morning_Table", 
                         con=sqlite3.connect("{}_Data.sqlite3".format(rCurrentDf['Pickup_Date'][1][0:7])))
        middayDf.to_sql(name="Midday_Table", 
                        con=sqlite3.connect("{}_Data.sqlite3".format(rCurrentDf['Pickup_Date'][1][0:7])))
        afternoonDf.to_sql(name="Afternoon_Table", 
                           con=sqlite3.connect("{}_Data.sqlite3".format(rCurrentDf['Pickup_Date'][1][0:7])))
        
        c.execute("""
                  SELECT name FROM sqlite_master
                  WHERE type='table';
                  """)
        print("The file {}_allDayDF.sqlite3 was successfully created".format(rCurrentDf['Pickup_Date'][1][0:7]),
              file=reportFile)
        print("Added tables to .sqltie3 file:", c.fetchall(), file=reportFile)
        print("\n", file=reportFile)
        
        c.close()
        conn.commit()
        conn.close()        
        
        # Plotting and creating a .png file to save the images (could be .jpg or .pdf as well)
        fig = plt.figure()
        fig.suptitle("Plotting requested taxi services for {}".format(rCurrentDf['Pickup_Date'][1][0:7]))
        
        x1 = plt.subplot2grid((10,14), (0,0), rowspan=4, colspan=6)
        x1.scatter(nightDf['Pickup_longitude'], nightDf['Pickup_latitude'], c='purple', alpha = 0.2)
        plt.xlim(-75, -73)
        plt.xlabel('Longitude')
        plt.ylim(40, 41)
        plt.ylabel('Latitude')
        plt.title('Night services for {}'.format(rCurrentDf['Pickup_Date'][1][0:7]))
        plt.grid(True)
        
        x2 = plt.subplot2grid((10,14), (0,8), rowspan=4, colspan=6)
        x2.scatter(morningDf['Pickup_longitude'], morningDf['Pickup_latitude'], c='g', alpha = 0.2)
        plt.xlim(-75, -73)
        plt.xlabel('Longitude')
        plt.ylim(40, 41)
        plt.ylabel('Latitude')
        plt.title('Morning services for {}'.format(rCurrentDf['Pickup_Date'][1][0:7]))
        plt.grid(True)
        
        x3 = plt.subplot2grid((10,14), (6,0), rowspan=4, colspan=6)
        x3.scatter(middayDf['Pickup_longitude'], middayDf['Pickup_latitude'], c='orange', alpha = 0.2)
        plt.xlim(-75, -73)
        plt.xlabel('Longitude')
        plt.ylim(40, 41)
        plt.ylabel('Latitude')
        plt.title('Midday services for {}'.format(rCurrentDf['Pickup_Date'][1][0:7]))
        plt.grid(True)
        
        x4 = plt.subplot2grid((10,14), (6,8), rowspan=4, colspan=6)
        x4.scatter(afternoonDf['Pickup_longitude'], afternoonDf['Pickup_latitude'], c='orangered', alpha = 0.2)
        plt.xlim(-75, -73)
        plt.xlabel('Longitude')
        plt.ylim(40, 41)
        plt.ylabel('Latitude')
        plt.title('Afternoon services for {}'.format(rCurrentDf['Pickup_Date'][1][0:7]))
        plt.grid(True)
        
        plt.savefig('RepImg_{}.png'.format(rCurrentDf['Pickup_Date'][1][0:7]))
        
# Creating a list of files to be moved and move them into the folder created for them
filesToBeMoved = [file_to_move for file_to_move in os.listdir("{}".format(workingPath)) 
    if not file_to_move.startswith("green_trip") and not file_to_move.endswith(".py")
    and not os.path.isdir(file_to_move)]

print("First List ->", filesToBeMoved)
print("Dst Fol ->", destinationFolder)

for movingFile in filesToBeMoved:
    os.rename("{}/".format(workingPath) + movingFile,
              "{}/".format(destinationFolder) + movingFile)
       
# Repeat
