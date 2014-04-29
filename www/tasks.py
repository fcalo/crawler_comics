#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, redirect, url_for, make_response, flash
from flask.ext.sqlalchemy import SQLAlchemy
from database import db_session
from models import Task
import time
import os

app = Flask(__name__)

@app.route('/tasks/', methods = ["GET", "POST"])
def tasks():
	if request.method == "POST":
		s_date = time.strftime("%Y-%m-%d %H:%M:00", time.strptime(request.form['start_date'], "%d/%m/%Y %H:%M"))
		task = Task(start_date = s_date, mode = 0 if "mode" in request.form else 1)
		db_session.add(task)
		
	#~ url_subq = session.query(URL).count(URL.url).group_by(URL.last_seen_task).subquery()
	#~ return render_template('tasks.html', tasks = Task.query.join(subq_url, TASK.id_taks = subq_url.last_seen_task).order_by("id_task DESC").limit(20).all())
	
	return render_template('tasks.html', tasks = Task.query.order_by("id_task DESC").limit(20).all())
    
@app.route('/remove/<id_task>')
def remove(id_task):
	db_session.query(Task).filter(Task.id_task == id_task).delete()
	
	return redirect(url_for('tasks'))
	
@app.route('/log/<id_task>')
def log(id_task):
	
	filename = app.config['FILE_LOG'] % id_task
	
	if os.path.exists(filename):
		response = make_response(open(filename).read())
		response.headers["Content-type"] = "text/plain"
		return response
	else:
		flash(u"Aún no se ha creado el fichero de Log")
		return redirect(url_for('tasks'))

@app.route('/csv/<id_task>')
def csv(id_task):
	filename = app.config['FILE_CSV'] % id_task
	
	if os.path.exists(filename):
		response = make_response(open(filename).read())
		response.headers["Content-type"] = "text/csv"
		response.headers["Content-Disposition"] = 'attachment; filename="%s"' % filename.split("/")[-1]
		return response
	else:
		flash(u"Aún no se ha creado el fichero csv")
		return redirect(url_for('tasks'))
	

app.debug = True
app.config.from_object("settings.Config")
app.secret_key = app.config['SECRET']
if __name__ == '__main__':
	app.run()
