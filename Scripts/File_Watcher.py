from os import listdir
from os.path import isfile, join
import os
import time
import re
import File_Ingester
import Run_Crawler
import Run_Glue_Job

#function to return files in a directory
#Scans for files in the format <name>_<source>_<locationcode>_<date>_<time>.<csv|txt> 
# name: alphabet sequence of no fixed length 
# source: alphabet sequence of 3 characters indicating source of data
# locationCode: 3 character long country code 
# date: in YYYYMMDD format 
# time: in HHMMSS 24-hour format  

def fileInDirectory(my_dir: str):
    onlyfiles = [f for f in listdir(my_dir) if isfile(join(my_dir, f))]
    index = 0
    accepted_formats = [".csv",".txt"]

    while index < len(onlyfiles):
        name,extension = os.path.splitext(onlyfiles[index])
        index+=1
        if extension not in accepted_formats:
            onlyfiles.remove(onlyfiles[index-1])
            index = 0
    
    finalFiles = []
    for curFile in onlyfiles:
        res = re.match("[a-zA-z]+_[a-zA-Z]{3}_[a-zA-Z]{3}_(?<!\d)(?:(?:20\d{2})(?:(?:(?:0[13578]|1[02])31)|(?:(?:0[1,3-9]|1[0-2])(?:29|30)))|(?:(?:20(?:0[48]|[2468][048]|[13579][26]))0229)|(?:20\d{2})(?:(?:0?[1-9])|(?:1[0-2]))(?:0?[1-9]|1\d|2[0-8]))(?!\d)_(?:(?:([01]?\d|2[0-3]))?([0-5]?\d))?([0-5]?\d)[.](txt)|[a-zA-z]+_[a-zA-Z]{3}_[a-zA-Z]{3}_(?<!\d)(?:(?:20\d{2})(?:(?:(?:0[13578]|1[02])31)|(?:(?:0[1,3-9]|1[0-2])(?:29|30)))|(?:(?:20(?:0[48]|[2468][048]|[13579][26]))0229)|(?:20\d{2})(?:(?:0?[1-9])|(?:1[0-2]))(?:0?[1-9]|1\d|2[0-8]))(?!\d)_(?:(?:([01]?\d|2[0-3]))?([0-5]?\d))?([0-5]?\d)[.](csv)",curFile)
        if res is not None:
            finalFiles.append(curFile)
 
    return(finalFiles)

def listComparison(OriginalList: list, NewList: list):
    differencesList = [x for x in NewList if x not in OriginalList] #Note if files get deleted, this will not highlight them
    return(differencesList)

def fileWatcher(my_dir: str, pollTime: int):
    while True:
        if 'watching' not in locals(): #Check if this is the first time the function has run
            previousFileList = fileInDirectory(my_dir)
            watching = 1
            print('First Time')
            File_Ingester.DataIngester(previousFileList)
            
        
        time.sleep(pollTime)
        
        newFileList = fileInDirectory(my_dir)
        
        fileDiff = listComparison(previousFileList, newFileList)
        
        previousFileList = newFileList
        if len(fileDiff) == 0: continue
        File_Ingester.DataIngester(fileDiff)

if __name__=="__main__":
    path = os.getcwd()
    fileWatcher(path, 100)