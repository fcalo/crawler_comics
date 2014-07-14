#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, logging, logging.handlers
from db import DB
import datetime
def next_weekday(weekday):
	d = datetime.datetime.now()
	days_ahead = weekday - d.weekday()
	if days_ahead <= 0: # Target day already happened this week
		days_ahead += 7
	return d + datetime.timedelta(days_ahead)


config_file = os.path.join(os.path.dirname(__file__), "crawler_comics.conf")

#logger
logger = logging.getLogger('CRAWLER_CRON')
hdlr = logging.handlers.TimedRotatingFileHandler(os.path.join(os.path.dirname(__file__), "logs/cron_crawler.log"),"d",2)
hdlr.suffix = "%Y-%m-%d-%H-%M"
formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)
logger.info("[cron] Comenzando")

db = DB(logger, config_file)

task = db.get_waiting_task()

if task:
	crawl_py = os.path.join(os.path.dirname(__file__), task['file'])

	logger.info("[cron] Lanzando tarea %s, %d, mode %d" % (crawl_py, task['id_task'], task['mode']))
	os.system("/usr/bin/python %s %d %d" % (crawl_py, task['id_task'], task['mode']))
	logger.info("[cron] ¿Es tarea automatica? %d" % task['id_task'])
	
	#weekday puede ser cero y da problemas
	if not task['week_day'] is None:
		start_date = next_weekday(task['week_day']).replace(hour=int(str(task['hour']).split(":")[0]), minute=int(str(task['hour']).split(":")[1]))
		db.create_auto_task(start_date, task['mode'], task['type_task'], task['week_day'], task['hour'])
	
	
	logger.info("[cron] Terminada tarea %d" % task['id_task'])
else:
	logger.info("[cron] Ninguna tarea pendiente")

