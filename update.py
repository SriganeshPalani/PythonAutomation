import re
import os

def parser(str_file):
	try:	
		regex = r"^\s*(UPDATE .*?)(FROM .*)(SET .*)(WHERE .* )+;"
		matches = re.match("(?msx)"+regex, str_file)  
	#####################################################
	
		if matches:
		
			re1=matches.group(1) 
			re2=matches.group(2) 
			re3=matches.group(3) 
			re4=matches.group(4) 
		#print('>>>>>>>>>>>>re1',re1)
		#print('>>>>>>>>>>>>re2',re2)
		#print('>>>>>>>>>>>>re3',re3)
		#print('>>>>>>>>>>>>re4',re4)		
	#####################################################
			regex11=r"UPDATE\s+(\S+)"
			match = re.match("(?msx)"+regex11,re1)
		#print('match',match.group(1))
			upd = match.group(1)
		#print("upd",upd)
			if re.search(upd,re2):
				#print("re",re2)
				#re11=r"(.*)"+upd
			#print(re11)
				tablematch1=re.match("(?msx)"+r"FROM.*\s+"+upd+"[,\s+]",re2)
			#print("tablematch1",tablematch1)
			#table1=tablematch1.group(1).split()[-1]
				#print("<<<",tablematch1.group())
				table1=' '.join(tablematch1.group().split()[-2:])
				table1=re.sub(r",","",table1)
			#print(' '.join(tablematch1.group().split()[-2:]))
				#print("table1",table1)
				result1='UPDATE '+table1
				result3=str(re2)
				result3=re.sub(r"(?msx)\S+\s+"+upd+"[,\s+]","",result3)
				result3=re.sub(r"(FROM)\s*,",r"\1 ",result3)
			else:
				#regex1=r"FROM\s+(\S*)\s+"
				#matches1=re.match("(?msx)"+regex1,re2)
				table1=str(upd)
				result1='UPDATE '+table1
				result3=str(re2)
		#print(table1, 
		#result1='UPDATE '+table1
		#print('>>>>>>>>>>>>>>>>>update',result1)

	#####################################################
	
		result2=str(re3)

	#####################################################
		
		#result3=str(re2)
		#result3=re.sub(r"(?msx)\S+\s+"+upd,"",result3)
		#result3=re.sub(r",","",result3)
		#print("result3",result3)

	#####################################################
	   
		result4=str(re4)
	#####################################################
		result_final=result1+'\n'+result2+result3+result4+';\n'
		#print("result1",result1)
		#print("result2",result2)
		#print("result3",result3)
		#print("result4",result4)
		#print("**************")
		return (result_final)
		
	except:
		#print("::::::::::::::::")
		return (str_file)
