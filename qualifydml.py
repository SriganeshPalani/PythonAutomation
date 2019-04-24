import re
import os

def parser(str_file):

	regex = r"^\s*(INSERT .*?)(SELECT  .*?)(FROM .*)(QUALIFY .*) ;$"
	matches = re.match(regex, str_file, flags=re.MULTILINE | re.VERBOSE |re.DOTALL|re.IGNORECASE)
	re1_str=str(matches.group(1))
	re2_str=str(matches.group(2))
	re3_str=str(matches.group(3))
	re4_str=str(matches.group(4))
	#print(re4_str)
	####################################################################
	regex1=r"INSERT(.*)INTO (.*)"
	match_table=re.match(regex1,re1_str,flags= re.MULTILINE | re.DOTALL)
	table=str(match_table.group(1))
	table=table.upper()
	re_mod1='INSERT INTO '+table
	####################################################################
	regex2=r"SELECT(.*)"
	matches2=re.match(regex2,re2_str,flags= re.MULTILINE | re.DOTALL)
	colnames=str(matches2.group(1))
	colnames=colnames.rstrip().split(',')
	colnames=[word.strip() for word in colnames]
	col_final=list()
	for col in colnames:
		col_final.append('TMP.'+col.upper())
	col=str(col_final)
	col_1=col.replace('[',' ')
	col_2=col_1.replace(']',' ')
	col_final=col_2.replace("'"," ")
	col_final=col_final.replace('"',' ')
	col_final=col_final.replace("\n"," ")
	re_mod2='SELECT '+col_final+'FROM '  #########REMOVED +'(\n'
	##########################################################################
	regex3=r"QUALIFY (.*)\)(.*)"
	matches3=re.match(regex3,re4_str,flags= re.MULTILINE | re.DOTALL| re.VERBOSE)
	qualify_str=str(matches3.group(1))
	qualify_str=qualify_str.rstrip()
	qualify_str=qualify_str.replace('\n','')
	qualify_value=str(matches3.group(2))
	qualify_value=qualify_value.replace('\n'," ")
	########################################################################
	first_col=colnames[0]
	colnames.insert(0,'SELECT '+first_col+' AS')
	colnames.append(qualify_str)
	re_mod3=str(tuple(colnames))
	re_mod3=re_mod3.replace("'","")
	re_mod3=re_mod3.replace('"',' ')
	index=re_mod3.find(",")
	a=re_mod3[0:index]
	b=re_mod3[index+1:]
	re_mod3=a+b
	re_mod3=re_mod3+' AS RANKVALUE'
	re_mod3=' '.join(re_mod3.split())
	#############################################################################
	re_mod_final=re_mod1+' '+re_mod2+re_mod3+' '+re3_str+' '+') '+ 'AS TMP '+'WHERE '+'TMP.RANKVALUE'+qualify_value+';\n'
	#print('qualifydml')
	#re_mod_final=re.sub(r", ROWNUM = 1\) AS RANKVALUE","",re_mod_final)
	#re_mod_final=re.sub(r"(AS TMP WHERE).*\s*;",r"\1 TMP.ROWNUM=1 )TMP",re_mod_final)
	re_mod_final=re_mod_final.replace("\n"," ")
	output_format=str()
	output_format=re_mod_final
	#output_format=str.encode(re_mod_final)
	return output_format
