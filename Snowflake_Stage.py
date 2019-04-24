import os
import sys
import re
import glob
from string import Template
import snowflake.connector

#--------Snowflake Connection----------------------#

cnx=snowflake.connector.connect(
user='spalani5',
password='Sriga@2013',          
account='jcpenney.us-east-1',
warehouse = 'LOAD_WH',
database = 'PLSQLTEST',
schema = 'PUBLIC',
role = 'sysadmin'
)

def snowflake_ReSA_tables():
    cursor = cnx.cursor()
    input_file_path =r"C:\Users\spalani5\Desktop\click_stream.json"
    var1 = Template("PUT file://$one @JSON_STAGE")
    var2 = var1.substitute(one=input_file_path)
    cnx.cursor().execute(var2)
    print("Total Number of file staged")

if __name__ ==  '__main__':
    print("Process Begin")
    snowflake_ReSA_tables()
    print("Process End")
