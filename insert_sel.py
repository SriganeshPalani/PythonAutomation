import re

def parser(insert_sql):
	matches_insert = re.match(r'\s*(INSERT\s+.*\)?.*?SELECT(.*?)\s+FROM)',insert_sql,flags = re.M|re.DOTALL)
	if matches_insert:
		pat2 = matches_insert.group(1)
		pat3 = matches_insert.group(2)
		if pat2:
			columns = matches_insert.group(2)
			colnames=columns.rstrip().split(',')
			z = ' '
			for i in colnames[:-1]:
				match = re.match('(.*?)(\(TRIM.*?\))(.*)',i)
				if re.search(r'^\s*TRIM',i,re.M) and i.count('TRIM') == 1 and re.search(r"[^'DW.CHKNUM']",i,re.M):
					j = i.split(' AS ')
					x = 'coalesce(' + j[0] + ",'')" + " AS " + j[1] 
					insert_sql = insert_sql.replace(i,x)
				elif re.search(r'TRIM',i,re.M) and i.count('TRIM') == 1 and match and re.search(r"[^'DW.CHKNUM']",i,re.M):
					x = match.group(1) + " (coalesce" + match.group(2) + ",'')" + match.group(3)
					insert_sql = insert_sql.replace(i,x)
				else:
					x = i 
					insert_sql = insert_sql.replace(i,x)
					
	return (insert_sql)

		
		