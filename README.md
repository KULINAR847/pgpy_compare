# pgpy_compare
Compare two databases using git

You need install your connections between two databases in postgresql_compare.py
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
run
python postgresql_compare.py
