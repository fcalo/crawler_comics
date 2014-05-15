from sqlalchemy import Column, Integer, String, Date
from database import Base

class Task(Base):
	__tablename__ = "task"
	id_task = Column(Integer, primary_key=True)
	start_date = Column(Date)
	type_task = Column(Integer)
	state = Column(Integer)
	mode = Column(Integer)
	
	def __init__(self, id_task = None, start_date = None, type_task = 0, mode = 0):
		self.id_task = id_task
		self.start_date = start_date
		self.type_task = type_task
		self.mode = mode
		self.state = 0
        #~ self.mode = mode
        #~ self.state = 0
        
	def __repr__(self):
		return '<Task %r>' % (self.id_task)

class TypeTask(Base):
	__tablename__ = "type_task"
	id_type_task = Column(Integer, primary_key=True)
	type_task = Column(Integer)
	
	def __init__(self, id_type_task = None, type_task = None):
		self.id_type_task = id_type_task
		self.type_task = type_task
        
	def __repr__(self):
		return self.type_task
