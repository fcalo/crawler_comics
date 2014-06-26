#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, logging, logging.handlers
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../crawlerSD/"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../crawler/"))

from db import DB

config_file = os.path.join(os.path.dirname(__file__), "newsletter.conf")

#logger
logger = logging.getLogger('NEWSLETTER_CRON')
hdlr = logging.handlers.TimedRotatingFileHandler(os.path.join(os.path.dirname(__file__), "logs/cron_newsletter.log"),"d",2)
hdlr.suffix = "%Y-%m-%d-%H-%M"
formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)
logger.info("[cron] Comenzando")

db = DB(logger, config_file)

newsletter = db.get_waiting_newsletter()

if newsletter:
	newsletter_py = os.path.join(os.path.dirname(__file__), "newsletter.py")

	logger.info("[cron] Lanzando tarea %s, %d" % (newsletter_py, newsletter['id_newsletter']))
	os.system("/usr/bin/python %s %d" % (newsletter_py, newsletter['id_newsletter']))
	logger.info("[cron] Terminada tarea %d" % task['id_newsletter'])
else:
	logger.info("[cron] Ninguna tarea pendiente")

