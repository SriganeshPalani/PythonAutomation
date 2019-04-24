import sys, os, config, keyring, snowflake.connector as sfc, logging
from logging.handlers import RotatingFileHandler
from time import time
from string import Template

#Log File properties
log_file = config.log_path
log_format ="%(asctime)s: %(levelname)s : %(message)s"

#Setting up log file here
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(log_format)
file_handler = RotatingFileHandler(log_file,maxBytes=10485760,backupCount=20)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

#Password Encryption using Keyring Module
pwd=keyring.get_password('snowflake',config.user)

# --Snowflake Connection Setup
cnx = sfc.connect(user=config.user, password=pwd, account=config.account, warehouse = config.warehouse, database=config.database, schema=config.schema, role = config.role)

# --Function sales_temp_load_ty      
def sales_temp_load_ty():
    logger.info("Function sales_temp_load_ty Begins")
    #---Function call get_tran_date() 
    get_tran_date_rval = get_tran_date()
    if not get_tran_date_rval:
        logger.info("Error in get_trans_date function")
        return 0
    else:
        
        for get_cal_val in get_tran_date_rval:
            lp_calendar_id = get_cal_val[0]
            lp_mcal_cal_wid = get_cal_val[1]
            lp_start_date = get_cal_val[2]
            lp_end_date = get_cal_val[3]
            lp_cur_year = get_cal_val[4]
            lp_cur_half = get_cal_val[5]
            lp_cur_qtr = get_cal_val[6]
            lp_cur_period = get_cal_val[7]
            lp_cur_week = get_cal_val[8]
            lp_cur_day = get_cal_val[9]
            lp_moth_last = get_cal_val[10]
            lp_qtr_start_dt = get_cal_val[11]
            lp_hlf_start_dt = get_cal_val[12]
            lp_year_start_dt = get_cal_val[13]
            lp_qtr_last_dt = get_cal_val[14]
            lp_hlf_last_dt = get_cal_val[15]
            lp_year_last_dt = get_cal_val[16]
            lp_prior_week = get_cal_val[17]
            lp_mth_start_dt = get_cal_val[18]
            lp_mth_last_dt = get_cal_val[19]
            lp_wk_start_dt = get_cal_val[20]
            lp_wk_last_dt  = get_cal_val[21]
            l_total_rows  = get_cal_val[22]
            lp_load_type = get_cal_val[23]
            
            
            l_var_sub1 = Template("\'$temp_sub\'")
            l_var_sub2 = l_var_sub1.substitute(temp_sub=lp_calendar_id)
    
            if lp_load_type is None:
                lp_load_type = 'R'
                if lp_load_type!= 'H':
                    
                    try:
                        with open(r"C:\Desktop\PLSQL_Test\03_Sep_Test\SQL\sales_temp_load_ty_Query01.txt","r") as sqlinput:
                            cursor = cnx.cursor()
                            sql_query = sqlinput.read().replace('lp_cur_day',str(lp_cur_day))                            
                            c_check_curr_day_data = cursor.execute(sql_query).fetchall()
                        if not c_check_curr_day_data:
                            print("Sales fact table does not have yesterday data : ", lp_cur_day)
                            cursor.close()
                            return 0
                        
                    except sfc.errors.ProgrammingError as e:
                        logger.info('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
                                   
            try:
                cursor = cnx.cursor()                
                cursor.execute("truncate table WC_RTL_SLS_SCLCDY_TY_TMP;")
                logger.info("Truncation - WC_RTL_SLS_SCLCDY_TY_TMP")
                
            except sfc.errors.ProgrammingError as e:
                logger.info('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            
            finally:
                cursor.close()
                
            try:
                with open(r"C:\Desktop\PLSQL_Test\03_Sep_Test\SQL\sales_temp_load_ty_Query02.txt",'r') as sql_data:
                    input_query = sql_data.read().replace('lp_mcal_cal_wid',str(lp_mcal_cal_wid)).replace('lp_start_date',str(lp_start_date )).replace('lp_end_date',str(lp_end_date)).replace('lp_calendar_id',str(l_var_sub2))

                    cursor = cnx.cursor()
                    loop_cursor=cursor.execute(input_query).fetchall()
                                        
                    for rec_month in loop_cursor:
                        rec_month_var = rec_month[0]
                        try:
                            with open(r"C:\Desktop\PLSQL_Test\03_Sep_Test\SQL\sales_temp_load_ty_Query03.txt", "r") as input_file:
                                input_data = input_file.read().replace('lp_mcal_cal_wid',str(lp_mcal_cal_wid)).replace('lp_start_date',str(lp_start_date )).replace('lp_end_date',str(lp_end_date)).replace('lp_calendar_id',str(l_var_sub2)).replace('rec_month.mcal_period_wid',str(rec_month_var))       
                                tic = time()
                                cursor = cnx.cursor()
                                insert_exe = cursor.execute(input_data).rowcount
                                toc = time()
                                print(tic,"-",toc)
                                print("insert completed with",insert_exe,"Rows")
                                
                        except sfc.errors.ProgrammingError as e:
                            logger.info('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
                        finally:
                             cursor.close()

            except sfc.errors.ProgrammingError as e:
                logger.info('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
                return 0     

            finally:
                cursor.close()
                logger.info("Function sales_temp_load_ty end")
                return 1

# -- Function get_tran_date
def get_tran_date():
    logger.info("Function get_tran_date Begins")    
    try:        
        with open(r"C:\Desktop\PLSQL_Test\03_Sep_Test\SQL\c_get_calender.txt","r") as sql_input:
            sql_query = sql_input.read()            
            cursor = cnx.cursor()
            c_get_calender = cursor.execute(sql_query).fetchall()
            if not c_get_calender:
                l_total_rows = 0
                #--- Function Call rtl_calendar_update
                rtl_calendar_update_rval = rtl_calendar_update()
                if rtl_calendar_update_rval == 0 or rtl_calendar_update_rval  == None:
                    logger.info("Error in function rtl_calendar_update.")        
                    return 0
                else:
                    l_total_rows = 0
                    io_error_message = None
                    try:
                        with open(r"C:\Desktop\PLSQL_Test\03_Sep_Test\SQL\c_get_calender.txt","r") as sql_input:
                            sql_query = sql_input.read()      
                            cursor = cnx.cursor()
                            c_get_calender = cursor.execute(sql_query).fetchall()
                            if not c_get_calender:
                                l_total_rows = 0
                                logger.info("No Date Range data for Transaction to Pull")
                                return 0
                            else:
                                for get_cal_val in c_get_calender:
                                    io_end_date = get_cal_val[3]
                                    l_total_rows  = get_cal_val[22]
                                    io_load_type = get_cal_val[23]
                                    
                                    if l_total_rows > 1:
                                        logger.info('More than one Quarter records for history load')
                                        return 0
                                        
                    except sfc.errors.ProgrammingError as e:
                        logger.info('Error {0} ({1} : {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

                    finally:
                        cursor.close()                                    
                                                
            else:            
                for get_cal_val in c_get_calender:     
                    io_end_date = get_cal_val[3]
                    l_total_rows  = get_cal_val[22]
                    io_load_type = get_cal_val[23]
                    
                    if l_total_rows > 1:
                        print('More than one Quarter records for history load')
                        return 0
                
    except sfc.errors.ProgrammingError as e:
        logger.info('Error {0} ({1} : {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

    finally:
        cursor.close()

    try:
        with open(r"C:\Desktop\PLSQL_Test\03_Sep_Test\SQL\c_get_future_date.txt") as sql_input:
            logger.info("Cursor - c_get_future_date.get_tran_date")
            c_get_future_date = sql_input.read().replace('io_end_date',str(io_end_date))      
            cursor = cnx.cursor()
            io_future_date = cursor.execute(c_get_future_date).fetchall()

            if io_load_type == 'H':
                io_future_date = io_end_date
                print(io_start_date,"-",io_end_date)
                            
    except sfc.errors.ProgrammingError as e:
        logger.info('Error {0} ({1} : {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

    finally:
        cursor.close()
    logger.info("Function c_get_future_date end")
    return c_get_calender
    
#--- Function rtl_calendar_update 
def rtl_calendar_update():
    #--- Function rtl_calendar_id call 
    function_rtcde = rtl_calendar_id()
    if function_rtcde[0] == 0:
        return 0

    lp_calendar_id = function_rtcde[1]
    l_var_sub1 = Template("\'$temp_sub\'")
    l_var_sub2 = l_var_sub1.substitute(temp_sub=lp_calendar_id)
    
    try:
        with open(r"C:\Desktop\PLSQL_Test\03_Sep_Test\SQL\rtl_calendar_update_Cur01.txt","r") as sql_input:
            sql_data = sql_input.read().replace('lp_calendar_id',str(l_var_sub2))
            cursor=cnx.cursor()
            cur_res = cursor.execute(sql_data).rowcount
            print("Table W_MCAL_DAY_D Updated completed : ",cur_res)
            
            
    except sfc.errors.ProgrammingError as e:
        logger.info('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

    try:
        with open(r"C:\Desktop\PLSQL_Test\03_Sep_Test\SQL\rtl_calendar_update_Cur02.txt","r") as sql_input:
            sql_data = sql_input.read().replace('lp_calendar_id',str(l_var_sub2))
            cursor=cnx.cursor()
            cur_res = cursor.execute(sql_data).rowcount
            print("Table W_MCAL_DAY_D Updated completed : ",cur_res)
            cursor.execute("COMMIT;")
            return 1
    except sfc.errors.ProgrammingError as e:
        logger.info('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        return e

    finally:
        cursor.close()

#--- Function rtl_calendar_id 
   
def rtl_calendar_id():
    
    try:
        c_get_calender = "SELECT PARAM_VALUE FROM TABLE WHERE PARAM_NAME = 'INPUT1' AND SCENARIO_NAME = 'INPUT2';"
        cursor=cnx.cursor()
        o_calendar_id = cursor.execute(c_get_calender).fetchone()
        logger.info(o_calendar_id)
        return (1,o_calendar_id[0])
	
        
    except sfc.errors.ProgrammingError as e:
        logger.info('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        return (0,e)
    finally:
        cursor.close()

if __name__ == "__main__":
    print("Sales_temp_load_ty process starting")    
    sales_temp_load_ty()
    print("Sales_temp_load_ty process Completed")

