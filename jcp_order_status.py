import sys
import os
import cx_Oracle

def jcp_order_status(jcp_order_number):

    """
        This function connect to JCP oracle database and retrive the Current status of the passed order.
            1. Order # mandatory argument to run this function

        Validation:
             This function included following validation before hitting oracle database:
                 # Check entered order # is 16 digit.           
    """

    con = cx_Oracle.connect('ster_93_uat/uat0210@comdbter1-scan.jcpenney.com:1531/OMCOTST1')
    
    try:
        soma_query = """
                                    SELECT 
                                        S.DESCRIPTION ,
                                            (
                                            CASE
                                                WHEN RS.STATUS<'3350.100'       
                                                    THEN 'STUCK IN SOMA'
                                                WHEN RS.STATUS='3350.100'       
                                                    THEN 'SENT TO FULFILLMENT NODES'
                                                WHEN RS.STATUS='9000'       
                                                    THEN 'CANCELLED'
                                                WHEN RS.STATUS>='3700' AND RS.STATUS<>'9000'       
                                                    THEN 'PROCESSED SUCCESSFULLY IN SOMA'
                                                END)  
                                        AS STATUS
                                    FROM 
                                        YFS_ORDER_HEADER OH, 
                                        YFS_ORDER_RELEASE_STATUS RS, 
                                        YFS_STATUS S
                                    WHERE 
                                        OH.ORDER_HEADER_KEY = RS.ORDER_HEADER_KEY  AND
                                        RS.STATUS = S.STATUS AND 
                                        S.PROCESS_TYPE_KEY = 'ORDER_FULFILLMENT' AND 
                                        RS.STATUS_QUANTITY > 0 AND 
                                        OH.ORDER_NO = 'io_order_number'
                                """
        final_query = soma_query.replace('io_order_number',str(jcp_order_number))
        cursor = con.cursor()
        order_details = cursor.execute(final_query).fetchall()              
        return(order_details)                   

    except Exception as e:     
        return(e)

    finally:
        con.close()
        

def efas_status(jcp_alloc_id):
    """
        This function connect to JCP oracle database and retrive the Current status of the Allocation.
            1. Allocation # mandatory argument to run this function

        Validation:
             This function included following validation before hitting oracle database:
                 # Check entered Allocation # is -- digit.           
    """

    cnx = cx_Oracle.connect('AB4P_APP01/ab4p_app01_stage@ltb1-scan.jcpenney.com:1521/NALSTG1')

    try:
        efas_quert = """
                                SELECT
                                        MODEL_STATUS_NAME
                                FROM
                                        MODEL_STATUS_TBL
                                WHERE
                                        MODEL_STATUS_ID 
                                                IN(
                                                        SELECT
                                                                MODEL_STATUS_ID
                                                        FROM
                                                                ALLOCS_TBL
                                                        WHERE
                                                                ALLOC_ID = 'IO_ALLOC_ID'
                                                )
                             """
        executable_query = efas_quert.replace('IO_ALLOC_ID',str(jcp_alloc_id))
        cursor = cnx.cursor()
        alloc_details = cursor.execute(executable_query).fetchall()
        alloc_output = "Allocation # "+str(body)+str(" ")+ str(alloc_val[0][0])
        return(alloc_output)
        

    except Exception as e:
        return(e)

    finally:
        cnx.close()
                   
#######-------------------Main Function-----------------###############

jcp_order_number = sys.argv[1]
jcp_alloc_id = sys.argv[2]

if len(jcp_order_number) != 16:
    print("check your order number")

else:
    ord_val = jcp_order_status(jcp_order_number)
    if not ord_val:
        print("Order not found in JCP")
    else:
        print("Mationed Order # ",ord_val[0][1],"and now it's in",ord_val[0][0],"Stautus")
    
    alloc_val = efas_status(jcp_alloc_id)
    
    if not alloc_val:
        print("Allocation not found in JCP")
    else:
        print("Allocation is: ",alloc_val[0][0])
    
    
    
    
    
