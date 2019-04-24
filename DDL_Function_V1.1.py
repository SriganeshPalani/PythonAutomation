# coding=utf8
# the above tag defines encoding for this document and is for Python 2.x compatibility
import re, sys, os, config, keyring, glob, snowflake.connector as sfc, logging
from logging.handlers import RotatingFileHandler

#  Log File properties
log_file = config.log_path
log_format ="%(asctime)s: %(levelname)s : %(message)s"

#  Setting up log file here
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(log_format)
file_handler = RotatingFileHandler(log_file,maxBytes=10485760,backupCount=20)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

#  Password Encryption using Keyring Module
pwd=keyring.get_password('snowflake',config.user)

# Snowflake Connection Setup
cnx = sfc.connect(user=config.user,
                  password=pwd,
                  account=config.account,
                  warehouse = config.warehouse,
                  database=config.database,
                  schema=config.schema,
                  role = config.role)


def ADW_TO_SNF(input_file,schema_sub):

    """"
        1. Function will match statments from CREATE to DISTRIBUTE.
        2. Used replace() function, replaced extra values.
        3. Regex substitute modules is used to append default values for the below conditions:
            i.SMALLINT|INTEGER|BIGINT|DECIMAL|NUMERIC|REAL|DOUBLE|DECFLOAT|FLOAT --> Default Value is 0
            ii.CHAR|GRAPHIC|BINARY|VARCHAR|CLOB|VARGRAPHIC|DBCLOB|VARBINARY|BLOB --> Default Value is " "
            iii.NOT NULL WITH DEFAULT COMPRESS SYSTEM DEFAULT --> NOT NULL WITH DEFAULT
            iv. DATE NOT NULL WITH DEFAULT '9999-01-01' --> DATE NOT NULL WITH DEFAULT 9999-01-01
        4. After all change, creates tables in snowflake under the database mentioned in config.py file
        5. All matched tables DDL will be written into log file for references.

        Note:
            A. This script will match only the DDL between the statement --> CREATE to DISTRIBUTE. If any extra values falls between this need to be replace.
            B. If any pattern falls other than point 3, need to include it.
            C.Make sure input file name should <sechema_name>.<referenec>(eg:RFRADWDDL.sql)

        Modules Required to Run the Scripts:
            A. Python with version 3.6.x or higher(https://www.python.org/downloads/release/python-370/)
            B. Snowflake Connector for Python(pip install snowflake-connector-python)
            C. Keyring(pip install keyring)
    """

    regex = r"^(CREATE TABLE)(\s\"[a-zA-z]+.*\"\.)(\".*\".+\((\n.+)+DISTRIBUTE)"
    
    try:          
        with open(input_file,"r") as db2_input:
            db2_str = db2_input.read()
            matches = re.finditer(regex, db2_str, re.MULTILINE)
            for matchNum, match in enumerate(matches):
                matchNum = matchNum + 1
                matched_str = match.group(1)
                matched_str2 = match.group(2)
                matched_str1 = match.group(3)
                #print("**********",matched_str2)

        

                mapped_ddl = matched_str+str(' ')+str(schema_sub)+str(matched_str1).replace(
                    "DISTRIBUTE",";").replace(
                    "WITH DEFAULT CURRENT TIMESTAMP","WITH DEFAULT CURRENT_TIMESTAMP").replace(
                    "COMPRESS SYSTEM DEFAULT"," ")

                formatting_ddl = re.sub(r'((CHAR|GRAPHIC|BINARY|VARCHAR|CLOB|VARGRAPHIC|DBCLOB|VARBINARY|BLOB)(\(.*|\s+)NOT\s+NULL\s+(WITH DEFAULT|DEFAULT)\s+)',r"\1 ' '",mapped_ddl).replace("  ' ''"," '")
                formatting_ddl1 = re.sub(r'((SMALLINT|INTEGER|BIGINT|DECIMAL|NUMERIC|REAL|DOUBLE|DECFLOAT|FLOAT)(\(.*|\s+)NOT\s+NULL\s+(WITH DEFAULT|DEFAULT)\s+)',r"\1 0",formatting_ddl)
                formatting_ddl2 = re.sub(r'IN\s\"+.*\"',r" ",formatting_ddl1)
                formatting_ddl3 = re.sub(r'((\s+|)COMPRESS YES)',r";",formatting_ddl2)
                formatting_ddl4 = re.sub(r'(DATE\s+NOT\s+NULL\sDEFAULT\s+)(\')(\d+.\d+.\d+)(\')',r"\1 \3",formatting_ddl3)
                
                logger.info(formatting_ddl4)
                
                #-----------------------Connection to Snowflake------------------------------#
                
                try:
                    cursor = cnx.cursor()
                    cursor_stmt = cursor.execute(formatting_ddl4).fetchall()
                    print("Snowflake: ",cursor_stmt[0])
                    
                except sfc.errors.ProgrammingError as e:
                    logger.info('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
                    
                finally:
                    cursor.close()
                
            print("No of Table Created from:",os.path.basename(input_file).split('.')[0],"is: ", matchNum)
            
    except Exception as e:
        print(e)

    

if __name__ == "__main__":
    input_file_path = "C:\\Users\\spalani5\\Desktop\\ADWtoSnowflake\\ADW_Input\\"+"/*"
    input_files=glob.glob(input_file_path)
    if (len(input_files) == 0):        
        print("No file to Proceed")
    for input_file in input_files:
        
        try:
            fname=os.path.basename(input_file).split('.')[0]
            print("Current Processing File: ",fname)
            
            if fname == "RFR":
                schema_sub = '"JCPSTG"."RFR_ADW".'
                ADW_TO_SNF(input_file,schema_sub)
            if fname == "CATA":
                schema_sub = '"JCPSTG"."CATADB".'
                ADW_TO_SNF(input_file,schema_sub)                
        
        except Exception as e:            
            logger.info(e)

 
    
