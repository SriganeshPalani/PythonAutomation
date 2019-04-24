import os
import sys
import re
import glob

def snowflake_ReSA_tables(in_data):
    with open("C:\\Users\\Ganesh\\Desktop\\DATA.CSV.txt","a+") as out_file:
        out_file.write(in_data)          
        

if __name__ ==  '__main__':
    input_file_path ="C:\\Users\\Ganesh\\Desktop\\Test_Folder\\ranks\\" + "\*"
    input_files= glob.glob(input_file_path)
    i = 0
    for input_fil in input_files:
        print(input_fil)
        try:
            i=+1
            with open(input_fil,"r") as in_file:
                in_data = in_file.read()
                print("*************")
                snowflake_ReSA_tables(in_data)
            
                #print("######")
            
                  
        except Exception as e:
            print(e)
        
        

