# pgpy_compare
Compare two databases using git

1. Packages
<code>
   pip install pg8000
</code>   
2. You need install your connections between two databases in postgresql_compare.py
<pre>
<code>
conn_src = {
	'user':'postgres', 
	'password':'11111',
	'database':'db1', 
	'host':'localhost', 
	'port':5432
}
conn_dst = {
	'user':'postgres', 
	'password':'11111',
	'database':'db2', 
	'host':'localhost', 
	'port':5432
}
</code>
</pre>
RUN
<code>
python postgresql_compare.py
</code>

Open git gui client and you can see diff between two postgresql databases in db folder. 
You can see also report_diff.txt
