import sys, os, config, keyring, snowflake.connector as sfc
from string import Template
import glob


#Password Encryption using Keyring Module
pwd=keyring.get_password('snowflake',config.user)

# --Snowflake Connection Setup
cnx = sfc.connect(user=config.user, password=pwd, account=config.account, warehouse = config.warehouse, database=config.database, schema=config.schema, role = config.role)

def get_table_list():
    cursor = cnx.cursor()
    get_table_list = cursor.execute("SHOW TABLES;").fetchall()
    i = 0
    for tables in get_table_list:
        i += 1
        l_var_sub1 = Template("SELECT GET_DDL('TABLE','$table_sub');")
        l_var_sub2 = l_var_sub1.substitute(table_sub=tables[1])
        get_table_ddl = cursor.execute(l_var_sub2).fetchall()
        try:
            with open("C:\\Users\\spalani5\\Desktop\\PLSQL_Test\\DDL\\"+str(tables[1])+".txt",'w') as output:
                output.write(str(get_table_ddl))

        except Exception as e:
            print(e)

    format_files()

def format_files():

    path = "C:\\Users\\spalani5\\Desktop\\PLSQL_Test\\DDL\\"

    for filename in os.listdir(path):
        file_path = (path+str(filename))
        try:
            with open(file_path,"r") as input:
                print(file_path)
                replaced_data = input.read().replace("\\n","\n").replace("\\t","\t").replace("[('"," ").replace("',)]"," ")
                try:
                    with open("C:\\Users\\spalani5\\Desktop\\PLSQL_Test\\DDL_Final\\"+str(filename),'w') as output:
                        output.write(replaced_data)
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)                    
                   
    
if __name__ == '__main__':

    print("process Begin")
    get_table_list()
    print("Process End")
    
    
