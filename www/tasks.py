#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, redirect, url_for, make_response, flash
from flask.ext.sqlalchemy import SQLAlchemy
from sqlalchemy.exc import OperationalError
from database import db_session
from models import Task, TypeTask, Newsletter, Category, NewsletterCategory
from datetime import datetime
import time
import os, re

app = Flask(__name__)

@app.route('/tasks/', methods = ["GET", "POST"])
def tasks():
	if request.method == "POST":
		
		
		s_date = time.strftime("%Y-%m-%d %H:%M:00", time.strptime(request.form['start_date'], "%d/%m/%Y %H:%M"))
		task = Task(start_date = s_date, type_task = request.form['type_task'], \
			mode = 0 if "mode" in request.form else 1, \
			week_day = request.form['week_day'] if "auto" in request.form and request.form['auto'] == "on" else None, \
			hour = request.form['hour'] if "auto" in request.form and request.form['auto'] == "on" else None)
		db_session.add(task)
		
	#~ url_subq = session.query(URL).count(URL.url).group_by(URL.last_seen_task).subquery()
	#~ return render_template('tasks.html', tasks = Task.query.join(subq_url, TASK.id_taks = subq_url.last_seen_task).order_by("id_task DESC").limit(20).all())
	for i in xrange(0,5):
		try:
			return render_template('tasks.html', tasks = Task.query.order_by("id_task DESC").limit(20).all(), type_tasks = TypeTask.query.order_by("id_type_task").all())
		except OperationalError:
			#retry
			pass
			time.sleep(i)
	raise Exception("Mysql has gone away")
		
    
@app.route('/remove/<id_task>')
def remove(id_task):
	db_session.query(Task).filter(Task.id_task == id_task).delete()
	
	return redirect(url_for('tasks'))


def get_filename(pattern, id_task):
	try:
		possible = re.findall(".*\[(.*)\].*", pattern)[0].split("|")
		fix = "{type}".join(re.findall("(.*)\[.*\](.*)", pattern)[0])
		
		for _type in possible:
			fn = fix.replace("{type}", _type) % id_task
			if os.path.exists(fn):
				return fn
	except:
		return pattern % id_task
	
	
	

@app.route('/log/<id_task>')
def log(id_task):
	
	filename = get_filename(app.config['FILE_LOG'], id_task)
	
	print filename
	
	if filename and os.path.exists(filename):
		response = make_response(open(filename).read())
		response.headers["Content-type"] = "text/plain"
		return response
	else:
		flash(u"Aún no se ha creado el fichero de Log")
		return redirect(url_for('tasks'))

@app.route('/csv/<id_task>')
def csv(id_task):
	filename = get_filename(app.config['FILE_CSV'], id_task)
	
	if filename and os.path.exists(filename):
		response = make_response(open(filename).read())
		response.headers["Content-type"] = "text/csv"
		response.headers["Content-Disposition"] = 'attachment; filename="%s"' % filename.split("/")[-1]
		return response
	else:
		flash(u"Aún no se ha creado el fichero csv")
		return redirect(url_for('tasks'))
	
#newsletter
@app.route('/newsletter', methods = ["GET", "POST"])
def newsletter():
    if request.method == "POST":
		
		newsletter = Newsletter(header_text = request.form['header_text'],
          banner_1_active = 'banner_1_active' in request.form, 
          banner_1_url = request.form['banner_1_url'], 
          banner_1_image = request.form['banner_1_image'], 
          banner_2_active = 'banner_2_active' in request.form, 
          banner_2_url = request.form['banner_2_url'], 
          banner_2_image = request.form['banner_2_image'], 
          banner_3_active = 'banner_3_active' in request.form, 
          banner_3_url = request.form['banner_3_url'], 
          banner_3_image = request.form['banner_3_image'], 
          banner_4_active = 'banner_4_active' in request.form, 
          banner_4_url = request.form['banner_4_url'], 
          banner_4_image = request.form['banner_4_image'], 
          type_link = request.form['type_link'], 
          id_affil = request.form['id_affil'], 
          template = request.form['template'], 
          date_from = datetime.strptime(request.form['date_from'].strip(), "%d/%m/%Y") if "/" in request.form['date_from'] else request.form['date_from'], 
          date_to = datetime.strptime(request.form['date_to'].strip(), "%d/%m/%Y") if "/" in request.form['date_to'] else request.form['date_to'])
		db_session.add(newsletter)
		db_session.flush()
		
		for cat in request.form['categories'].split(","):
			n_c = NewsletterCategory(id_newsletter = newsletter.id_newsletter, category = cat.strip())
			db_session.add(n_c)
			db_session.flush()
		
    for i in xrange(0,5):
        try:
			return render_template('newsletter.html', newsletters = Newsletter.query.order_by("id_newsletter DESC").limit(50).all(), 
			  categories = [unicode(str(c),"latin-1") for c in Category.query.order_by("category").all()])
        except OperationalError:
            #retry
            pass
            time.sleep(i)
    raise Exception("Mysql has gone away")
    
@app.route('/remove_news/<id_newsletter>')
def remove_news(id_newsletter):
	db_session.query(Newsletter).filter(Newsletter.id_newsletter == id_newsletter).delete()
	
	return redirect(url_for('newsletter'))

@app.route('/log_news/<id_newsletter>')
def log_news(id_newsletter):
	
	filename = get_filename(app.config['FILE_LOG_NEWS'], id_newsletter)
	
	if filename and os.path.exists(filename):
		response = make_response(open(filename).read())
		response.headers["Content-type"] = "text/plain"
		return response
	else:
		flash(u"Aún no se ha creado el fichero de Log")
		return redirect(url_for('newsletter'))

@app.route('/html/<id_newsletter>')
def html(id_newsletter):
	filename = get_filename(app.config['FILE_HTML'], id_newsletter)
	
	print filename
	
	if filename and os.path.exists(filename):
		response = make_response(open(filename).read())
		response.headers["Content-type"] = "application/force-download"
		response.headers["Content-Disposition"] = 'attachment; filename="%s"' % filename.split("/")[-1]
		return response
	else:
		flash(u"Aún no se ha creado el fichero")
		return redirect(url_for('newsletter'))


app.debug = True
app.config.from_object("settings.Config")
app.secret_key = app.config['SECRET']
if __name__ == '__main__':
	app.run()
