# pgpy_compare
Compare two databases using git and Windows

1. Packages
<pre>
<code>
   pip install pg8000
</code>  
</pre>
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
<pre>
<code>
python postgresql_compare.py
</code>
</pre>
Open git gui client and you can see diff between two postgresql databases in db folder. 
You can see also report_diff.txt

You can see the difference in the database not only of your changes, but also of the database versions.
![diff_base](https://github.com/user-attachments/assets/304dadaf-352b-4f2d-98c2-3727fff41ab9)
