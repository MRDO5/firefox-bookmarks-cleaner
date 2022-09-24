import sqlite3
import requests

import traceback
import sys


def execute_query(cursor, query):
	try:
		#sqlite_connection = sqlite3.connect('places.sqlite')
		#cursor = sqlite_connection.cursor()
		cursor.execute(query)
		print("Подключен к SQLite")
	except sqlite3.Error as error:
		print("Ошибка при работе с SQLite", error)
	finally:
		if sqlite_connection:
			#print("Total, removed from table moz_bookmarks after connecting to the database:" , sqlite_connection.total_changes)
			#sqlite_connection.close()
			print("Соединение с SQLite закрыто")

	# finally:
	# 	if sqlite_connection:
	# 		sqlite_connection.close()
	# 		print("Соединение с SQLite закрыто")
	# try:
	#     cursor.execute(query)
	# except Exception as error:
	#     print(str(error) + "\n " + query)


def read_sqlite_table(cursor, url, places_id):

	sqlite_select_query = """select moz_places.id, url, moz_places.title, rev_host, frecency, last_visit_date from moz_places  join  \
							  moz_bookmarks on moz_bookmarks.fk=moz_places.id where visit_count>=0
							  and moz_places.url like 'http%' order by dateAdded desc;"""
	execute_query(cursor, sqlite_select_query)
	records = cursor.fetchall()
	print("Всего строк:", len(records))

	for row in records:
		ids = row[0]
		url_links = row[1]
		#print(ids)
		#print(url_links)
		places_id.append(ids)
		url.append(url_links)
	#cursor.close()

	# except sqlite3.Error as error:
	# 	#print("Не удалось вставить данные в таблицу sqlite")
	# 	print("Класс исключения: ", error.__class__)
	# 	print("Исключение", error.args)
	# 	print("Печать подробноcтей исключения SQLite: ")
	# 	exc_type, exc_value, exc_tb = sys.exc_info()
	# 	print(traceback.format_exception(exc_type, exc_value, exc_tb))
	# finally:
	#     if sqlite_connection:
	#         sqlite_connection.close()
	#         print("Соединение с SQLite закрыто")

	get_url_request(url, broken_url)
	#delete_multiple_records(places_id)

def get_url_request(url, broken_url):
	try:
		for link in url:
			url = link
			#print(url)
			resp = requests.get(url)
			cod_answer = resp.status_code
			if cod_answer != 200:
				broken_url.append(url)
            elif cod_answer == 301 or cod_answer == 302:
                pass
            elif cod_answer == 404:
				broken_url.append(url)
			#answer = resp.status_code
			#print(answer)
			#print(resp.status_code)
	except Exception as e:
		pass
	#else:
	#	print(answer)
	delete_multiple_records(broken_url, cursor)


def delete_multiple_records(broken_url, cursor):

	for url in broken_url:
		#print(url)
		#sqlite_select_ids = """select id from moz_places where url = https://www.cyberciti.biz/faq/how-to-add-network-bridge-with-nmcli-networkmanager-on-linux/"""
		sqlite_select_ids = f"select id from moz_places where url = '{url}'"
		#print(sqlite_select_ids)
		#query2 = sqlite_select_ids, url,
		#print(query2)
		execute_query(cursor, sqlite_select_ids)
		#cursor.execute(sqlite_select_ids, (url,))
		records = cursor.fetchall()
		#print(records)
		for row in records:
			#print("ID:", row[0])
			id = row[0]
			#print(id)
			#sql_delete_query = """DELETE from moz_bookmarks where fk = ?"""
			sql_delete_query = f"DELETE from moz_bookmarks where fk = '{id}'"
			#print(sql_delete_query)
		##	query3 = sql_delete_query, (id, )
			#execute_query(cursor, query3)
			execute_query(cursor, sql_delete_query)
			#cursor.execute(sql_delete_query, (id, ))
			#print("Удалено записей:", cursor.rowcount )

		sqlite_connection.commit()
		print("The record was successfully deleted")
	print("Total, removed from table moz_bookmarks after connecting to the database:" , sqlite_connection.total_changes)
		#cursor.close()


url = []
places_id = []
broken_url = []

sqlite_path = 'places.sqlite'
sqlite_connection = sqlite3.connect(sqlite_path)
cursor = sqlite_connection.cursor()


read_sqlite_table(cursor, url, places_id)
cursor.close()
sqlite_connection.close()

# get_url_request(url, broken_url)
# delete_multiple_records(broken_url, cursor)
# cursor.close()

#get_bookmarks(cursor)
#cursor.close()
