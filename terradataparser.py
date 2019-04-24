import re
import os
import config
import shutil
import glob
from modules import update
from modules import update_set
from modules import qualifyddl
from modules import qualifydml
from modules import snowsqlsubs
from modules import insert_sel

pattern = config.pattern
env_parameters = config.env_dict
#pattern="\s+"+pattern.replace("|","|\s+")
# regex = r"^(?:" + pattern + r")(?:[^;']|(?:'[^']+'))+;"
#regex = r"^(?:" + pattern + r")(?:[^;']|(?:'[^']+'))+;"

regex = r"^(?:" + pattern + r")(?:[^;]|(?:'.*?'))*;"


#regex = regex.replace("|","|\s*")
input_file_path= config.input_file_path
output_file_path = config.snow_sql_output_path

def numericalSort(value):
	file_name=os.path.basename(value)
	part_match=re.match(r'^(\d+)_.*sql',file_name)
	parts=part_match.group(1)
	return int(parts)
				
def terradata_parser(file):
	with open(file, 'r') as myfile:
		str_file=myfile.read()
		str_file=re.sub(r'(?ms)(^)?[^\S\n]*/(?:\*(.*?)\*/[^\S\n]*|/[^\n]*)($)?','\n',str_file)
		matches = re.finditer("(?ims)"+regex, str_file) 
		return matches

def snowflake_data(data,ipname,input_file_full_path):
	  
	update_regex=r"^\s*UPDATE.*SELECT.*SET" ####################
	qualify_regex1=r"^\s*CREATE.*QUALIFY" ###############
	qualify_regex2=r"^\s*INSERT.*QUALIFY"#########################
	
	with open(input_file_full_path,'r') as f:
		fin = f.read().upper()
	
	#fin = snowsqlsubs.replace_key_words(fin)
	final_snow_sql = str()
	
	for match in data:
		match_str = match.group()
		match_str = match_str.strip().upper()
	
		if re.search('^\s*UPDATE',match_str,re.MULTILINE):
		
			matches = re.match("(?msx)"+update_regex, match_str)	
			#if matches:
				#update_data=update_set.parser(match_str)
			#else:
			update_data=update.parser(match_str)
			
			fin = fin.replace(match_str,update_data)
		
		if re.search(r"^\s*(?:CREATE|INSERT).*QUALIFY",match_str,re.DOTALL):
		
			matches_qualify1 = re.match("(?msx)"+qualify_regex1,match_str)
			matches_qualify2 = re.match("(?imsx)"+qualify_regex2,match_str)
 
			if matches_qualify1:
			
				qualify_data_ddl=qualifyddl.parser(match_str)
				
				fin = fin.replace(match_str,qualify_data_ddl)
				
			if matches_qualify2:
			
				sql_file=re.sub(r"(?m)-.*\s,?"," ",match_str)
				qualify_data_dml=qualifydml.parser(match_str)
				
				fin = fin.replace(match_str,qualify_data_dml)
				
				
		if re.search(r"(?ms)^\s*(?:CREATE\s*SET|CREATE\s*MULTISET)\s+VOLATILE TABLE",match_str):
			volatile_data=re.sub(r"MULTISET\s+(VOLATILE)",r"\1",match_str)
			volatile_data=re.sub(r"SET\s+(VOLATILE)",r"\1",volatile_data)
			volatile_data = re.sub(r"(?ms)(CREATE\s+VOLATILE\s+TABLE\s+\w+)(.*?)(\(.*)",r"\1\3",volatile_data)
			#print(volatile_data)
			fin = fin.replace(match_str,volatile_data)
		if re.search(r"(?ms)^\s*(?:CREATE\s*VOLATILE\s*SET|CREATE\s*VOLATILE\s*MULTISET)\s+TABLE",match_str):
			#print(':::::::::::')
			#print(match_str)
			volatile_data=re.sub(r"(VOLATILE)\s+MULTISET",r"\1 ",match_str)
			volatile_data=re.sub(r"(VOLATILE)\s+SET",r"\1 ",match_str)
			#volatile_data=re.sub(r"(VOLATILE)\s+MULTISET",r"\1",match_str)
			volatile_data = re.sub(r"(?ms)(CREATE\s+VOLATILE\s+TABLE\s+\w+)(.*?)(\(.*)",r"\1\3",volatile_data)
			#print(volatile_data)
			fin = fin.replace(match_str,volatile_data)
			
		if re.search('INSERT',match_str,re.IGNORECASE):
			matches_insert = re.match(r'(?ms)\s*(INSERT\s+.*\)?.*?SELECT(.*?)\s+FROM)',match_str)
			
			if matches_insert:
				#print('insert pattern')
				insert_data = insert_sel.parser(match_str)
				fin = fin.replace(match_str,insert_data)
				
				fin = re.sub('DW.CHKNUM','TRY_TO_DECIMAL',fin)
				fin = re.sub(r'(TRY_TO_DECIMAL)(.*)\s*=0',r"\1\2IS NOT NULL",fin)
	 
	#fin = re.sub(r"(?mi)^\s*\.*?^*","",fin)
	#fin - re.sub(r"(?mi)[/*+ APPEND */]","",fin)
	fin = re.sub(r"/(.*)APPEND(.*)/","",fin)
	fin = re.sub(r"/*(.*)/","",fin)
	fin = re.sub(r"     TEST THAT SOURCE IS NOT EMPTY","",fin)
	fin = re.sub("[*]","",fin)
	fin = re.sub(r"\.IF ERRORCODE.*?;","",fin)
	fin = re.sub(".QUIT ERRORCODE","",fin)
	#SET QUERY_BAND
	fin = re.sub(r"(?mxi)SET QUERY_BAND\s*='.*'","",fin)
	fin = re.sub(r"(?mi)^\s*\.QUIT\s+.*?;","",fin)
	fin = re.sub(r"(?mi)^\s*\.QUIT","",fin)
	fin = re.sub(r"(?mi)\s*\.LOGOFF","",fin)
	fin = re.sub(r"(?mi)\s*EXEC\s+.*?;","",fin)
	fin = re.sub(r"(?mi)\s*\.SET WIDTH \S+","",fin)
	fin = re.sub(r"(?mi)\s*SET\s+.*?;","",fin)
	fin = re.sub(r"(?mi)\s*COLLECT STATISTICS\s+.*?;","",fin)
	fin = re.sub(r"(?mi)\s*COLLECT STATS\s+.*?;","",fin)
	fin = re.sub("DIAGNOSTIC\s+\S+\s+ON FOR SESSION;","",fin)
	#fin = re.sub(r"(?mi)^\s*BEGIN\s+.*;","",fin)
	fin = re.sub(r"(?msi)^\s*CALL\s+.*?;","",fin)
	fin = re.sub(r"(?mi)^\s*END\s+.*?;","",fin)
	fin = re.sub(r"(?mi)^\s*ESP_JOB_NM.*?;","",fin)
	fin = re.sub(r"(?mi)^\s*JOB_NAME.*?;","",fin)
	fin = re.sub(r"(?msi)^\s*JOB_SHORT_DESC.*?;","",fin)
	fin = re.sub(r"(?mi)^\s*APPLICATION_NAME.*?;","",fin)
	fin = re.sub(r"(?mi)^\s*JOB_NAME.*?;","",fin)
	fin = re.sub(r"(?mi)\s*FOR SESSION.*?;","",fin)
	fin = re.sub(r"(?mi)\s*bteq << EOF","",fin)
	fin = re.sub(r"(?mi)\s*EOF","",fin)
	fin = re.sub(r"(?mi)\s*#","\n",fin)
	fin = re.sub(r"(?msi)^\s*BTEQ\s+.*?;'","",fin)
	fin = re.sub(r"(?mxi)SET QUERY_BAND\s*='.*'","",fin)
	fin = re.sub(r"SA_","DWD_COMMON.DIV.SA_",fin)
	
	#fin = re.sub(r"(?mi)\s*# BTEQ ","",fin)
	#fin = re.sub(r"(?mi)\s*AUTO-GENERATED CODE","",fin)
	#fin = re.sub(r"(?mi)\s.LOGON","",fin)
	#fin = re.sub(r"(?mxi)\s*DATABASE","",fin)
	#fin = re.sub(r"(?mxi)$USER1,","",fin)
	#fin = re.sub(r"(?mxi)$PASS1,","",fin)
	
	
	#####temporary
	#fin=re.sub(r"#\$TERADATA.*#",'#$SNOWFLAKE_DATABASENAME#.TEMPDB',fin)
	#$TERADATA_WORK_DB#
	#fin=re.sub(r"#\$TERADATA_WORK_DB#",'#$SNOWFLAKE_WORK_DB',fin)
	#fin=re.sub(r"\$TERADATA(\S+)",r'$SNOWFLAKE\1',fin)
	for td_env,sf_env in env_parameters.items():
		fin=re.sub(td_env,sf_env,fin)

	################
	final_snow_sql = snowsqlsubs.replace_key_words(fin)
	

	
	with open(os.path.join(output_file_path,ipname+'.snow'),'w+') as fout:
		#fout.write('''\nuse warehouse #$SNOWFLAKE_WAREHOUSE#;
#use database #$SNOWFLAKE_DATABASENAME#;
#!set exit_on_error=True\n\n''')
		fout.write(final_snow_sql)

 
	# comment = """
# /**********************************************************************************************************
 # *  JOB Category	- Jobs\COREMETRICS\Snowflake
 # *  SCRIPT:		-Scriptname.snow
 # *  SCRIPT DIR:	- /ascential/Project_plus/GDW_PROD/sql/
 # *  PURPOSE:	  Loads a Target table from Work table.
 # *
 # *  MODIFICATION LOG:
 # *  2017-12-10  TCS *
  # *********************************************************************************************************
 # *  PURPOSE:	 This script loads the Master table from Work table for Coremetrics application in Snowflake.
  # ***********************************************************************************************************/

# use warehouse #$SNOWFLAKE_WAREHOUSE#;
# use database #$SNOWFLAKE_DATABASENAME#;
# !set exit_on_error=True
# !set variable_substitution=true
# !define year=2018
# """
	# comment = re.sub('Scriptname',ipname,comment)	  

	
				
		
				
				
				
					
