import re
def replace_key_words(snowsql):

	#snowsql=re.sub("CURRENT_TIMESTAMP\((\S+)\)",r"TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP(\1))",snowsql)
	snowsql=re.sub("CURRENT_TIMESTAMP(\S*)",r"TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP\1)",snowsql)
	snowsql=re.sub("TIMESTAMP\s*('.*')",r"TO_TIMESTAMP(\1)",snowsql)
	snowsql=re.sub(r"^DEL \s+","DELETE ",snowsql)
	snowsql=re.sub(r"^SEL \s+","SELECT ",snowsql)
	snowsql=re.sub("(?i)^SELECT\s*(?:getqueryband\(\);\s*|session;\s*)","",snowsql)
	snowsql=re.sub(r"(DELETE\s+FROM\s+\S+)\s+ALL",r"\1",snowsql)
	snowsql=re.sub("(?imsx)T.SESSIONNO\s*FROM\s*\(SELECT\s*SESSION\)\s*T\s*\(SESSIONNO\)","CURRENT_SESSION()",snowsql)

	snowsql=re.sub("DATE TO_DATE\('0001-01-01'\)","TO_DATE('0001-01-01')",snowsql)
	#snowsql=re.sub("DATE \('0001-01-01'\)","TO_DATE('0001-01-01')",snowsql)
	snowsql=re.sub(r"DATE ('\d\d\d\d-\d\d-\d\d')",r"TO_DATE(\1)",snowsql)
	
	#snowsql=re.sub(r"('\d\d\d\d-\d\d-\d\d')",r"TO_DATE(\1)",snowsql)
	#snowsql = re.sub(r"DATE\s*-\s*(\d+)",r"CURRENT_DATE - \1",snowsql)
	#snowsql = re.sub(r"DATE\s*\+\s*(\d+)",r"CURRENT_DATE \+ \1",snowsql)

	snowsql=snowsql.replace('\\N','')
	snowsql=snowsql.replace('\\T','')
	
	#TRIM(LEADING '0' FROM A.SKU_NUM)
	snowsql=re.sub(r"TRIM\(LEADING\s+('\S+')\s+FROM\s+(\S+)\)",r"LTRIM(\2,\1)",snowsql)
	snowsql=re.sub(r"(\S+)\s+MOD\s+(\S+)",r"MOD(\1,\2)",snowsql)
	snowsql=re.sub(r"TRUNC\s*\((\S+_TS)\)",r"CAST(\1 AS DATE)",snowsql)
	###date arithmetic handling
	#DATE - (366*3) - 7
	snowsql=re.sub(r"DATE\s*-\s*(\(\d+\s*\*\s*\d+\)\s*-\s*\d+)",r"DATEADD(DAY,-\1,DATE)",snowsql)

	snowsql=re.sub(r"DATE\s*-\s*(\d+)",r"DATEADD(DAY,-\1,DATE)",snowsql)
	snowsql=re.sub(r"DATE\s*\+\s*(\d+)",r"DATEADD(DAY,+\1,DATE)",snowsql)

	snowsql=re.sub(r"CURRENT_DATE\s*-\s*(\d+)",r"DATEADD(DAY,-\1,CURRENT_DATE)",snowsql)   ###CURRENT_DATE - <NUMBER>
	snowsql=re.sub(r"CURRENT_DATE\s*\+\s*(\d+)",r"DATEADD(DAY,+\1,CURRENT_DATE)",snowsql)	###CURRENT_DATE + <NUMBER>

	snowsql=re.sub(r"(?:\()CURRENT_DATE\s*-\s*(\S+_DT)",r"DATEDIFF(DAY, CURRENT_DATE , \1)",snowsql)  ##CURRENT_DATE - <DATE COLUMN VALUE>

	snowsql=re.sub(r"(MIN\(\S+_DT\))\s*(-\s*\d+)",r"DATEADD(DAY,\2,\1)",snowsql)   ##MIN(<DATE COLUMN VALUE>) - <NUMBER>
	snowsql=re.sub(r"(MIN\(\S+_DT\))\s*(\+\s*\d+)",r"DATEADD(DAY,\2,\1)",snowsql)
	snowsql=re.sub(r"(MIN\(DT\))\s*-(\s*\d+)",r"DATEADD(DAY,-\2,\1)",snowsql)	##MIN(<DATE COLUMN VALUE>) - <NUMBER>
	snowsql=re.sub(r"(MIN\(DT\))\s*\+(\s*\d+)",r"DATEADD(DAY,\2,\1)",snowsql)	##MIN(<DATE COLUMN VALUE>) - <NUMBER>
	


	snowsql=re.sub(r"(MAX\(\S+_DT\))\s*(-\s*\d+)",r"DATEADD(DAY,\2,\1)",snowsql)   ##MAX(<DATE COLUMN VALUE>) - <NUMBER>
	snowsql=re.sub(r"(MAX\(\S+_DT\))\s*(\+\s*\d+)",r"DATEADD(DAY,\2,\1)",snowsql)
	snowsql=re.sub(r"(MAX\(DT\))\s*-(\s*\d+)",r"DATEADD(DAY,-\2,\1)",snowsql)
	snowsql=re.sub(r"(MAX\(DT\))\s*\+(\s*\d+)",r"DATEADD(DAY,\2,\1)",snowsql)	##MAX(<DATE COLUMN VALUE>) - <NUMBER>

	snowsql=re.sub(r"(\S+_DT)\s*(-\s*\d+)",r"DATEADD(DAY,\2,\1)",snowsql)  ##(<DATE COLUMN VALUE> - <NUMBER>)
	snowsql=re.sub(r"(\S+\.DT)\s*-\s*(\S+_DT)",r"DATEDIFF(DAY,\1,\2)",snowsql) ##<DATE COLUMN VALUE> - <DATE COLUMN VALUE>
	snowsql=re.sub(r"(\S+_DT)\s*-\s*(\S+_DT)",r"DATEDIFF(DAY,\1,\2)",snowsql) ##
	#snowsql=re.sub(r"(\S+_DT)\s*-(\S+_DT)",r"DATEDIFF(DAY,\1,\2)
	snowsql=re.sub(r"CURRENT_DATE\s*-\s*INTERVAL\s+'(\d+)'\s+(\S+)",r"DATEADD(\2,-\1,CURRENT_DATE)",snowsql)
	###
	if re.match("(?m)DELETE\s+(\S+)",snowsql):	
		if re.match("(?m)DELETE\s+(\S+)",snowsql).group(1):
			if not("FROM" in re.match("(?m)DELETE\s+(\S+)",snowsql).group(1)):
				snowsql=re.sub("(DELETE)\s+(\S+)",r"\1 "+"FROM "+r"\2",snowsql)
				
	subs = ("CHARACTER SET LATIN NOT CASESPECIFIC","FORMAT 'YYYY/MM/DD'","FORMAT 'YYYY-MM-DD'","CHECKSUM = DEFAULT\s*,","DEFAULT MERGEBLOCKRATIO\s*,")
	
	snowsql=re.sub(r"UNIQUE\s+PRIMARY\s+INDEX\s*\(.*\)","",snowsql)
	
	snowsql=re.sub(r"PRIMARY INDEX\s*\(.*\)","",snowsql)
	snowsql=re.sub(r"NO\s+PRIMARY\s+INDEX","",snowsql)
	###user defined funcs
	snowsql = re.sub('DW.CHKNUM','TRY_TO_DECIMAL',snowsql)
	
	snowsql = re.sub('STRTOK','SPLIT_PART',snowsql)
	
	snowsql=re.sub("CHARACTER SET LATIN NOT CASESPECIFIC","",snowsql)	
	snowsql=re.sub("FORMAT 'YYYY-MM-DD'","",snowsql)	
	snowsql=re.sub("FORMAT 'YYYY/MM/DD'","",snowsql)
	snowsql=re.sub("CHECKSUM = DEFAULT\s*,","",snowsql)
	snowsql=re.sub("DEFAULT MERGEBLOCKRATIO\s*,","",snowsql)	

	for sub in subs:
		final_snowsql=re.sub(sub,"",snowsql)
			
	return final_snowsql
