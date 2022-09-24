import sqlite3
import requests

import traceback
import sys


def read_sqlite_table(url, places_id):
    try:
        sqlite_connection = sqlite3.connect('places.sqlite')
        cursor = sqlite_connection.cursor()
        print("Connected to SQLite")
        #sqlite_select_query = """ SELECT id, fk, title from moz_bookmarks where parent>2 and title!='Most Visited' and title!='Red Hat' join mozila_place on moz_bookmarks.fk=moz_places.id"""
        #sqlite_select_query = """SELECT moz_bookmarks.id, moz_bookmarks.fk, moz_bookmarks.title from moz_bookmarks join moz_places on moz_bookmarks.fk=moz_places.id """
        sqlite_select_query = """select moz_places.id, url, moz_places.title, rev_host, frecency, last_visit_date from moz_places  join  \
                              moz_bookmarks on moz_bookmarks.fk=moz_places.id where visit_count>=0
                              and moz_places.url like 'http%' order by dateAdded desc;"""
        cursor.execute(sqlite_select_query)
        records = cursor.fetchall()
        print("Total lines:  ", len(records))

        for row in records:
            ids = row[0]
            url_links = row[1]
            places_id.append(ids)
            url.append(url_links)
        cursor.close()

    except sqlite3.Error as error:
    	#print("Не удалось вставить данные в таблицу sqlite")
    	print("Exception class: ", error.__class__)
    	print("Exception", error.args)
    	print("Printing SQLite exception details: ")
    	exc_type, exc_value, exc_tb = sys.exc_info()
    	print(traceback.format_exception(exc_type, exc_value, exc_tb))
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            print("Connection to SQLite is closed")

    get_url_request(url, broken_url)


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
			#print(resp.status_code)
	except Exception as e:
		pass
	#else:
	#	print(answer)
	
	delete_multiple_records(broken_url)


def delete_multiple_records(broken_url):
    try:
        sqlite_connection = sqlite3.connect('places.sqlite')
        cursor = sqlite_connection.cursor()
        print("Connected to SQLite")

        for url in broken_url:
        	sqlite_select_ids = """select id from moz_places where url = ?"""
        	cursor.execute(sqlite_select_ids, (url,))
        	records = cursor.fetchall()
        	#print(records)
        	for row in records:
        	 	#print("ID:", row[0])
        	 	id = row[0]
        	 	#print(id)
        	 	sql_delete_query = """DELETE from moz_bookmarks where fk = ?"""
        	 	cursor.execute(sql_delete_query, (id, ))
        	#print("Удалено записей:", cursor.rowcount )

        sqlite_connection.commit()
        print("The record was successfully deleted")
        cursor.close()
    except sqlite3.Error as error:
        print("Error when working with SQLite", error)
    finally:
        if sqlite_connection:
        	print("Total, removed from table moz_bookmarks after connecting to the database:" , sqlite_connection.total_changes)
        	sqlite_connection.close()
        	print("Connection to SQLite is closed")


url = []
places_id = []
broken_url = []

read_sqlite_table(url, places_id)
print(broken_url)




