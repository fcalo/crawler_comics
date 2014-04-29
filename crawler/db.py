#!/usr/bin/env python
# -*- coding: utf-8 -*-

from warnings import filterwarnings
import MySQLdb
import MySQLdb.cursors
from MySQLdb import IntegrityError
from binascii import crc32
from pprint import pprint

from utils import get_title_collection, get_number_collection

filterwarnings('ignore', category = MySQLdb.Warning)


class DB(object):
	"""handle db access"""
	def __init__(self, logger, conf_file):
		
		self.logger = logger
		self.logger.info("[DB.__init__]")
		self.config = {}
		execfile(conf_file, self.config)
		
		self.con = MySQLdb.connect(self.config['mysql_host'], self.config['mysql_user'], self.config['mysql_pass'], cursorclass=MySQLdb.cursors.DictCursor)
		self.con.autocommit(True)

		#if not root fail:
		
		self.cur = self.con.cursor()
		if self.config['create_model']:
			self.cur.execute("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET =utf8 " % self.config['mysql_db'])
		self.con.select_db(self.config['mysql_db'])
		self.cur = self.con.cursor()
		
		self.con.set_character_set('utf8')
		self.cur.execute('SET NAMES utf8;')
		self.cur.execute('SET CHARACTER SET utf8;')
		self.cur.execute('SET character_set_connection=utf8;')
		
	def init_model(self):
		
		self.logger.info("[DB.init_model]")
		
		if self.config['create_model']:
			
			#task table
			sql = "CREATE TABLE IF NOT EXISTS task \
			  ( id_task int(6) NOT NULL AUTO_INCREMENT PRIMARY KEY, \
			  start_date DATETIME, state INT(1), mode INT(1)) CHARACTER SET=utf8;"

			self.cur.execute(sql)
			  
			#url table
			sql = "CREATE TABLE IF NOT EXISTS url ( \
			  url VARCHAR(255) PRIMARY KEY, \
			  crc_content int(4), \
			  last_seen_date datetime, \
			  last_seen_task int(6) , \
			  last_mod_task int(6) , %s, \
			  INDEX last_seen_task (last_seen_task) \
			  ) CHARACTER SET=utf8;" % \
			  ",".join(["%s TEXT NULL" % k for k in self.config['csv_header']])
			  
			self.cur.execute(sql)
			self.con.commit()
		
	def start_new_task(self):
		""" create new task and return id """
		
		self.logger.info("[DB.start_new_task]")
		self.cur.execute("INSERT INTO task (start_date, state) VALUES ( NOW(), 0);")
		_id = self.con.insert_id()
		
		self.con.commit()
		self.logger.info("[DB.start_new_task] creada tarea %d" % _id)
		return _id
		
	def save_data(self, url, metas, id_task):
		""" save all data for url """
		
		#~ pprint(metas)
		
		self.logger.info("[DB.save_data] salvando datos para %s" % url)
		try:
			sql = u"INSERT INTO url (url, crc_content, last_seen_date, last_seen_task, %s)\
			  VALUES ('%s', %d, NOW(), %d, '%s')" %  \
			  (",".join([k for k in self.config['csv_header'] if k in metas]), \
			  url, crc32(metas['content']), id_task, \
			  "', '".join([metas[k].decode("utf-8") if isinstance(metas[k], basestring) else str(metas[k]) for k in self.config['csv_header'] if k in metas]))
		except:
			pprint(metas)
			raise
		  
		  
		try:
			self.cur.execute(sql)
		except IntegrityError:
			#modification
			self.cur.execute("DELETE FROM url WHERE url = '%s'" % url)
			#one field more
			sql_mod = sql.replace(", last_seen_task,", ", last_seen_task, last_mod_task,")
			sql_mod = sql_mod.replace("NOW(), %d," % id_task, "NOW(), %d, %d," % (id_task, id_task)) 
			self.cur.execute(sql_mod)
			
		return self.con.commit()
		  
	def refresh_seen(self, url, id_task):
		""" update last seen for url """
		
		self.logger.info("[DB.refresh_seen] %s" % url)
		
		self.cur.execute("UPDATE url set last_seen_date = NOW(), last_seen_task = %d \
		  WHERE url = '%s'" % (id_task, url))
		
		return self.con.commit()
		
	def load_data(self, url):
		""" return data for url if exists"""
		
		self.logger.info("[DB.load_data] intentando recuperar data para %s" % url)
		
		metas = None
		
		self.cur.execute("SELECT * FROM url WHERE url = '%s'" % url)
		
		try:
			data = self.cur.fetchall()[0]
		except IndexError:
			return False
		
		metas = {k:data[k] if isinstance(data[k], basestring) else data[k] \
		  for k in self.config['csv_header'] + ['crc_content'] if k in data and data[k]}
		
		return metas
		
	def get_data_task(self, id_task, complete):
		""" return all data collected by one task """
		self.logger.info("[DB.get_data_task] devolviendo datos de la tarea %s" % id_task)
		
		if complete:
			self.cur.execute("SELECT * FROM url WHERE last_seen_task = %s ORDER BY last_seen_date ASC" % id_task)
		else:
			self.cur.execute("SELECT * FROM url WHERE last_mod_task = %s ORDER BY last_seen_date ASC" % id_task)
			#TODO: deletes
			#self.cur.execute("SELECT * FROM url WHERE last_seen_task < %s ORDER BY last_seen_date ASC" % id_task)
			
		
		return self.cur.fetchall()
	
	def get_data_task_removed(self, id_task):
		""" return all data collected by one task """
		self.logger.info("[DB.get_data_task] devolviendo datos de los borrados de la tarea %s" % id_task)
		
		self.cur.execute("SELECT * FROM url WHERE last_seen_task < (%s - 1) ORDER BY last_seen_date ASC" % id_task)
		
		return self.cur.fetchall()
		
	
	def get_same_collection(self, title_collection, number_collection, id_task, asc = True):
		""" search the titles for the same collection """
		
		self.cur.execute("SELECT id, title FROM url WHERE title like '%s%%' and last_seen_task = '%s'" % (title_collection, id_task))
		
		same = {}
		for data in self.cur.fetchall():
			if number_collection:
				n_collection = get_number_collection(data['title'])
				if (asc and n_collection > number_collection) or \
				 (not asc and n_collection < number_collection):
					same[n_collection] = data['id']
			else:
				same[len(same)] = data['id']
		
		if not same:
			return None
			
			
		print same
		print sorted(same)
		print list(reversed(sorted(same)))
		
		sorted_keys = list(reversed(sorted(same))) if asc else sorted(same)
		
		return [same[k] for k in sorted_keys[:6]]
				
	
		
	def get_related(self, title_collection, number_collection, id_task):
		""" search the titles for the same collection """
		
		self.logger.info("[DB.get_related] buscando relacionados %s - %s(%s)" % (title_collection, number_collection, id_task))
		return self.get_same_collection(title_collection, number_collection, id_task, True)
	
	def get_accesories(self, title_collection, number_collection, id_task):
		""" search the titles for the same collection """
		
		self.logger.info("[DB.get_related] buscando accesories %s - %s(%s)" % (title_collection, number_collection, id_task))
		return self.get_same_collection(title_collection, number_collection, id_task, False)
		
		
	def init_task(self, id_task):
		""" change state to running (1) to task """
		
		self.logger.info("[DB.init_task] %s" % id_task)
		
		self.cur.execute("UPDATE task set state=1 WHERE id_task = '%s'" % (id_task))
		
		return self.con.commit()
		
	def finish_task(self, id_task, with_errors = False):
		""" change state to finised with errors (2) or finised without errors to task """
		
		self.logger.info("[DB.finish_task] %s con errores : %s" % (id_task , with_errors))
		
		self.cur.execute("UPDATE task set state=%d WHERE id_task = '%s'" % (2 if with_errors else 3, id_task))
		
		return self.con.commit()
		
		
	def get_waiting_task(self):
		""" search for the task in state waiting """
		self.cur.execute("SELECT count(*) c FROM task WHERE state = 1")
		if self.cur.fetchall()[0]['c'] > 0:
			#another running
			return None
		
		self.cur.execute("SELECT * FROM task WHERE state = 0 order by id_task")
		try:
			return self.cur.fetchall()[0]
		except IndexError:
			return None
		
		
		
		
	
		
		
	
		
		
