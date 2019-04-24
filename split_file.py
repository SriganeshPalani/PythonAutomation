import sys, os
from fsplit.filesplit import FileSplit

def splitfile():

#Source file local path:
    input_file = r'C:\Users\spalani5\Downloads\chicago-311-service-requests\311-service-requests-abandoned-vehicles.csv'

#Splitsize - Specify the size you want to split in bytes
    fs = FileSplit(file=input_file, splitsize=1000000, output_dir = 'C:\\Users\\spalani5\\Desktop\\NextGen_BI\\Output')
    fs.split()   
    

if __name__ == "__main__":
    print("File Split Starts")
    splitfile()
    print("File Split Ends")
