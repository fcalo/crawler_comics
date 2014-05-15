#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, logging, logging.handlers
from db import DB

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
	logger.info("[cron] Terminada tarea %d" % task['id_task'])
else:
	logger.info("[cron] Ninguna tarea pendiente")

