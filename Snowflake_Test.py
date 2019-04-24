import sys, os, snowflake.connector as sfc


# ------------------------Connecting to Snowflake-----------------------------------------------#
cnx = sfc.connect(
    user='spalani5',
    password='Sriga@2013',
    account='jcpenney.us-east-1',
    warehouse = 'JCP_SUPPLY_COMPUTE',
    database='JCPSTG',
    schema='PUBLIC',
    role='sysadmin'
)


# --------------------Main Function Begins Here------------------------------------------------#   

def rtl_calendar_id():
    
    try:
        c_get_calender = "SELECT PARAM_VALUE FROM C_ODI_PARAM WHERE PARAM_NAME = 'CALENDAR_ID' AND SCENARIO_NAME = 'GLOBAL';"
        cursor=cnx.cursor()
        o_calendar_id = cursor.execute(c_get_calender).fetchall()
        print(o_calendar_id)
        return (1,o_calendar_id)
	
        
    except sfc.errors.ProgrammingError as e:
        print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        print(e)
        return (0,e)
    finally:
        cursor.close()
        



if __name__ == '__main__':
    print("sales_temp_load_ty function Begins")
    o_calendar_id_rc, o_calendar_id = rtl_calendar_id()
    print(o_calendar_id_rc, o_calendar_id)
    print("sales_temp_load_ty function End")






