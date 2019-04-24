import re
import os

def parser(str_file):
	
	regex = r"^(REPLACE .*)(SELECT  .*)(LEFT .* ;$)"
	matches = re.match("(?msx)"+regex, str_file)  	
	re1_str=str(matches.group(1))
	re2_str=str(matches.group(2))
	re3_str=str(matches.group(3))
	#########################################
	schema1=re2_str.strip().split(' ')[-1]
	res=re.sub('LOCKING ROW FOR ACCESS','WITH '+schema1+' AS',re1_str)
	re1_mod ='CREATE OR '+res
	########################################
	regex1=r"SELECT (.*) ,RANK()"
	matches1=re.match("(?imsx)"+regex1,re2_str)
	a1=(str(matches1.group(1)))
	var=schema1+'.'
	res1=re.sub(var,' ',a1)
	res1=res1.split()[0]
	########################################
	res2=re.sub(schema1+r"\.",' ',re2_str)
	res2=res2.split()[0:-1]
	res2=" ".join(res2)
	res2_mod='('+res2+')'
	########################################
	regex2=r"(SELECT .*) ,RANK()"
	matches2=re.match("(?imsx)"+regex2,re2_str)
	a2=(str(matches2.group(1)))
	res2_mod1=res2_mod+'\n'+a2+',' 
	########################################
	regex3=r"LEFT .* QUALIFY (.*);$"
	matches3=re.match("(?imsx)"+regex3,re3_str)
	rank=str(matches3.group(1))
	rank=rank.strip()
	rank1=rank.strip().split("=")[0]
	re2_mod=res2_mod1+schema1+'.'+rank1+'\nFROM '+schema1
	########################################
	regex4=r"(LEFT .*) QUALIFY"
	matches4=re.match("(?imsx)"+regex4,re3_str)
	res3_mod=str(matches4.group(1))
	re3_mod=res3_mod+'WHERE '+schema1+'.'+rank +';'
	output_format=re1_mod+re2_mod+'\n'+re3_mod
	
	return str(output_format)