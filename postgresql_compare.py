import pg8000
import os, re, time
import shutil
from subprocess import Popen, PIPE
### Получение текста запроса вьюшки postgresql
# SELECT
#     routine_definition 
# FROM
#     information_schema.routines 
# WHERE
#     specific_schema LIKE 'public'
#     AND routine_name LIKE 'functionName';


# Получение текста функций postgresql
# select n.nspname as function_schema,
#        p.proname as function_name,
#        l.lanname as function_language,
#        case when l.lanname = 'internal' then p.prosrc
#             else pg_get_functiondef(p.oid)
#             end as definition,
#        pg_get_function_arguments(p.oid) as function_arguments,
#        t.typname as return_type
# from pg_proc p
# left join pg_namespace n on p.pronamespace = n.oid
# left join pg_language l on p.prolang = l.oid
# left join pg_type t on t.oid = p.prorettype 
# where n.nspname not in ('pg_catalog', 'information_schema')

# Просмотреть список схем 
# select * from pg_namespace

# Посмотреть все вьюхи
# select * from information_schema.views

# Просмотреть все триггеры
# select * from information_schema.triggers
# exclude = ['trigger_catalog', 'event_object_catalog']
# SELECT row_number() OVER(order by n.nspname, c.relname), n.nspname, c.relname
# 														FROM pg_class c
# 														INNER JOIN pg_namespace n ON (n.oid = c.relnamespace)
# 														WHERE  c.relkind = 'r' order by n.nspname, c.relname;

### git получить список файлов с изменениями игнорируя пробелы
# git diff --stat --ignore-all-space --ignore-blank-lines
# git status --porcelain

### Алгоритм сравнения
# 1 вынимаем все функции в папку func из первой базы
# 2 делаем git commit -m 'dev'
# 3 очищаем папку func
# 4 вынимаем все функции в папку func из второй базы
# 5 смотрим объединение --porcelain и --stat --ignore-all-space

class PostgresConnector:
	def __init__(self, conn_src, conn_dst):
		# Объявим константы для работы
		self.pwd = os.getcwd()
		self.db_dir = 'db'
		self.git_dir = os.path.join(self.pwd, self.db_dir)
		print(self.git_dir)

		print('# 1. Создадим подключения к двум базам')
		self.init_connector_db(conn_src, conn_dst)

		print('# 2. Очистим каталог db и пересоздадим')
		if os.path.exists(self.git_dir):
			try:
				self.get_subprocess_answer('git_delete.bat', [self.git_dir])
			except:
				pass
			shutil.rmtree(self.git_dir)
		
		if not os.path.exists(self.git_dir):
			os.makedirs(self.git_dir)
		time.sleep(2)

		print('# 3. Сделаем инициализацию git репозитория')
		self.get_subprocess_answer('git_init.bat', [self.git_dir])

		print('# 4. Сохраним структуру первой БД')
		self.get_tables_info(self.cursor1)
		self.get_indexes(self.cursor1)
		self.get_sequences(self.cursor1)
		self.get_triggers(self.cursor1)
		self.get_view_info_from_db(self.cursor1)
		self.get_func_info_from_db(self.cursor1)		

		print('# 5. Сделаем коммит')
		self.get_subprocess_answer('git_commit.bat', [self.git_dir])

		print('# 6. Не забудем очистить, чтобы увидеть того чего не хватает')
		nodes = os.listdir(self.git_dir)
		for node in nodes:
			if node == '.git':
				continue
			full_path = os.path.join(self.git_dir, node)
			if os.path.isfile(full_path):
				os.remove(full_path)  
			elif os.path.isdir(full_path):  
				shutil.rmtree(full_path)  

		print('# 7. Сохраним структуру второй БД')
		self.get_tables_info(self.cursor2)
		self.get_indexes(self.cursor2)
		self.get_sequences(self.cursor2)
		self.get_triggers(self.cursor2)
		self.get_view_info_from_db(self.cursor2)
		self.get_func_info_from_db(self.cursor2)

		print('# 8. Соберём отчёт')
		text = self.get_subprocess_answer('git_porcelain.bat', [self.git_dir])
		#print(text.decode())

		res_porc = re.findall(r'[? ][?A-Z][ ].*', text.decode())
		#print(res)

		text = self.get_subprocess_answer('git_diff.bat', [self.git_dir])
		#print(text.decode())
		res_diff = re.findall(r'.*[|].*', text.decode())
        
		for r in res_porc:
			if len(r) > 3:
				if r[0:3] != ' M ':
					res_diff.append(r)

		text = self.get_subprocess_answer('git_file_diff.bat', [self.git_dir, 'indexes.txt'])
		#print(text.decode())

		res_indexes = re.findall(r'[+-]\[.*', text.decode())
		res_diff = res_diff + ['############################', r'# Индексы', '############################'] + res_indexes

		text = self.get_subprocess_answer('git_file_diff.bat', [self.git_dir, 'sequences.txt'])
		#print(text.decode())

		res_sequences = re.findall(r'[+-]\[.*', text.decode())
		res_diff = res_diff + ['############################', r'# Последовательности', '############################'] + res_sequences

		text = self.get_subprocess_answer('git_file_diff.bat', [self.git_dir, 'tables.txt'])
		#print(text.decode())

		res_sequences = re.findall(r'[+-]\[.*', text.decode())
		res_diff = res_diff + ['############################',r'# Таблицы и Вьюхи', '############################'] + res_sequences
	#print(self.conn_data1['database'])
		#print(self.conn_data1['host'])
		res_diff = ['[' + self.conn_data1['database'] + '] - > ' + '[' + self.conn_data2['database'] + ']', ''] + res_diff
		res_diff = ['[' + self.conn_data1['host'] + '] - > ' + '[' + self.conn_data2['host'] + ']'] + res_diff
		
		with open('report_diff.txt', 'w') as f:
			f.write('\n'.join(res_diff))
		
		# 1 Получим список схем 
		#sql = '''SELECT * FROM information_schema.schemata''' # полное сравнение
		# sql = '''SELECT schema_name, schema_owner FROM information_schema.schemata WHERE schema_name not like 'pg_%' ORDER BY 1''' # Частичное сравнение
		# h1, r1 = self.get_all_results(sql, self.cursor1) 
		# h2, r2 = self.get_all_results(sql, self.cursor2) 



		# 2 Получим текст запросов функций
		# sql = '''select n.nspname as function_schema,
		# 			p.proname as function_name,
		# 			l.lanname as function_language,
		# 			case when l.lanname = 'internal' then p.prosrc
		# 					else pg_get_functiondef(p.oid)
		# 					end as definition,
		# 			pg_get_function_arguments(p.oid) as function_arguments,
		# 			t.typname as return_type
		# 		from pg_proc p
		# 		left join pg_namespace n on p.pronamespace = n.oid
		# 		left join pg_language l on p.prolang = l.oid
		# 		left join pg_type t on t.oid = p.prorettype 
		# 		where n.nspname not in ('pg_catalog', 'information_schema')'''
		# h1, r1 = self.get_all_results(sql, self.cursor1) 
		# h2, r2 = self.get_all_results(sql, self.cursor2) 

		# h = h2
		# for r in r2:
		# 	self.save_func_data(r[h.index('function_schema')], r[h.index('function_name')], r[h.index('definition')], 'funcs')


		# Получим все вьюхи
		# sql = '''select * from information_schema.views'''

		# h1, r1 = self.get_all_results(sql, self.cursor1) 
		# h2, r2 = self.get_all_results(sql, self.cursor2) 

		# h = h1
		# for r in r1:
		# 	self.save_func_data(r[h.index('table_schema')], r[h.index('table_name')], r[h.index('view_definition')], 'views')


		# print(h1)
		# print(r1)

		# print(h2)
		# print(r2)

		# self.compare_elements(r2, r1, 3)

		#self.save_list_data(r2)
#numeric_precision_radix, numeric_scale, datetime_precision, interval_type, interval_precision,
	def get_tables_info(self, cursor):
		print(' - Получаем информацию о таблицах')
		#sql = '''SELECT * 
		sql = '''SELECT table_schema, table_name, column_name, table_catalog, udt_catalog,
					column_default, is_nullable, data_type, character_maximum_length, 
					character_octet_length, numeric_precision, 
					 is_updatable
					FROM information_schema.columns 
					WHERE table_schema NOT IN 
						('pg_catalog', 'information_schema')
					ORDER BY table_schema, table_name, column_name '''
		
		exclude_columns = ['table_catalog', 'udt_catalog']
		h, res = self.get_all_results(sql, cursor)
		#print(h)
		exclude_columns_indexes = list(map(lambda x: h.index(x), exclude_columns))
		#print(res[0])
		new_res = []
		for e in res:
			new_res.append([ str(element) for i,element in enumerate(e) if i not in exclude_columns_indexes])

		self.save_list_data(new_res, 'tables.txt')

	def get_indexes(self, cursor):
		print(' - Получаем информацию о индексах')
		sql = '''SELECT * FROM pg_indexes WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
				ORDER BY 1,2,3'''
		h, res = self.get_all_results(sql, cursor)
		self.save_list_data(res, 'indexes.txt')

	def get_sequences(self, cursor):
		print(' - Получаем информацию о последовательностях')
		sql = '''SELECT * FROM information_schema.sequences ORDER BY sequence_schema, sequence_name'''
		exclude_columns = ['sequence_catalog']
		h, res = self.get_all_results(sql, cursor)
		exclude_columns_indexes = list(map(lambda x: h.index(x), exclude_columns))
		#print(res[0])
		new_res = []
		for e in res:
			new_res.append([element for i,element in enumerate(e) if i not in exclude_columns_indexes])

		self.save_list_data(new_res, 'sequences.txt')

	def get_triggers(self, cursor):
		print(' - Получаем информацию о триггерах')
		sql = '''SELECT * FROM information_schema.triggers ORDER BY 2,3,4'''
		exclude_columns = ['trigger_catalog', 'event_object_catalog']
		h, res = self.get_all_results(sql, cursor)
		# exclude_columns_indexes = list(map(lambda x: h.index(x), exclude_columns))
		# print(res[0])
		# new_res = []
		# for e in res:
		# 	new_res.append([element for i,element in enumerate(e) if i not in exclude_columns_indexes])

		#self.save_list_data(new_res, 'triggers.txt')
		#print(new_res[0])

		for r in res:
			trigger_name = r[h.index('trigger_schema')] + '.' + r[h.index('trigger_name')]
			trigger_func = r[h.index('event_manipulation')] + '.' + r[h.index('event_object_schema')] + '.' + r[h.index('event_object_table')]
			data = r[h.index('action_statement')] + '\n' + r[h.index('action_timing')] + '\n' + r[h.index('action_orientation')]
			self.save_func_data(trigger_name, trigger_func, data, 'triggers')
		

	def get_view_info_from_db(self, cursor):
		print(' - Получаем информацию о вьюхах')
		sql = '''select * from information_schema.views'''
		h, res = self.get_all_results(sql, cursor)
		for r in res:
			self.save_func_data(r[h.index('table_schema')], r[h.index('table_name')], r[h.index('view_definition')], 'views')

	def get_func_info_from_db(self, cursor):
		print(' - Получаем информацию о функциях')
		sql = '''select n.nspname as function_schema,
					p.proname as function_name,
					l.lanname as function_language,
					case when l.lanname = 'internal' then p.prosrc
							else pg_get_functiondef(p.oid)
							end as definition,
					pg_get_function_arguments(p.oid) as function_arguments,
					t.typname as return_type
				from pg_proc p
				left join pg_namespace n on p.pronamespace = n.oid
				left join pg_language l on p.prolang = l.oid
				left join pg_type t on t.oid = p.prorettype 
				where n.nspname not in ('pg_catalog', 'information_schema')'''
		h, res = self.get_all_results(sql, cursor) 
		for r in res:
			self.save_func_data(r[h.index('function_schema')], r[h.index('function_name')], r[h.index('definition')], 'funcs')

	def compare_elements(self, el1, el2, show_columns=None):
		all_compared = True
		for e1 in el1:
			if e1 not in el2:
				if show_columns is not None:
					print(e1[:show_columns], 'diff1')
				else:
					print(e1, 'diff1')
				all_compared = False

		for e2 in el2:
			if e2 not in el1:
				if show_columns is not None:
					print(e2[:show_columns], 'diff2')
				else:
					print(e2, 'diff2')
				all_compared = False
		print('Compared result = ' + str(all_compared))

	def save_func_data(self, schema, name, data, folder):
		dst_folder = os.path.join(self.db_dir, folder) 
		if not os.path.exists(dst_folder):
			os.makedirs(dst_folder)

		with open(os.path.join(dst_folder, schema + '.' + name + '.txt'), "w") as f:
			f.write(str(data))
	
	def save_list_data(self, data, filename):
		big_str = ''
		for e in data:
			big_str = big_str + str(e) + '\n'
		
		if not os.path.exists(self.db_dir):
			os.makedirs(self.db_dir)

		with open(os.path.join(self.db_dir, filename), "w") as f:
			f.write(big_str)
	
	def __del__(self):
		self.conn1.commit()
		self.cursor1.close()
		self.conn1.close()

		self.conn2.commit()
		self.cursor2.close()
		self.conn2.close()

	def init_connector_db(self, conn_src, conn_dst):
		self.conn_data1 = conn_src
		self.conn1 = pg8000.connect(**self.conn_data1)
		self.cursor1 = self.conn1.cursor()

		self.conn_data2 = conn_dst
		self.conn2 = pg8000.connect(**self.conn_data2)
		self.cursor2 = self.conn2.cursor()
	
	def get_all_results(self, sql, cursor):
		try:
			cursor.execute(sql)
		except:
			return [], []
		#print(self.cursor.description)
		return [e[0] for e in cursor.description ], cursor.fetchall()
	
	def get_subprocess_answer(self, bat_file = '', args = [], cwd=None):
		if cwd is not None:
			pipe = Popen([bat_file] + args, cwd=cwd, stdout=PIPE)
		else:
			pipe = Popen([bat_file] + args, stdout=PIPE)
		return pipe.communicate()[0]

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
pc = PostgresConnector(conn_src, conn_dst)

