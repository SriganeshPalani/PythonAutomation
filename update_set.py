import re
import os



def parser(str_file):

	
	regex = r"^\s*(UPDATE .*?)(FROM .*)(SET .*)(WHERE .* )+;"
	matches = re.match("(?msx)"+regex, str_file)  

	re1=matches.group(1) if matches else ""
	re2=matches.group(2) if matches else ""
	re3=matches.group(3) if matches else ""
	re4=matches.group(4) if matches else ""
	
	regex1=r"FROM\s+(\S*)\s+"
	matches1=re.match("(?msx)"+regex1,re2)
	table1=str(matches1.group(1)) if matches else "" 
	result1='UPDATE '+table1	
	#print('>>>>>>>>>>>>>>>>>updateset',result1)


###########################################################


	# regex1=r"\s*UPDATE\s*(\S+)"
	# matches1=re.match("(?msx)"+regex1,re1)

	
	# table1=matches1.group(1) 
	# result1='UPDATE'+'\n'+table1
	# print(table1)

############################################################

	result2=str(re3)

############################################################

	regex2=r"FROM .*(\(SELECT .*)"
	matches2=re.match("(?msx)"+regex2,re2,flags=re.MULTILINE | re.VERBOSE |re.DOTALL)
	sel_match=str(matches2.group(1))
	result3='FROM\n'+sel_match

#############################################################

	result4=str(re4)+';\n'

#############################################################

	result_final=result1+'\n'+result2+result3+result4
	return str(result_final)

##############################################################

