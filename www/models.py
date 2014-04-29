from sqlalchemy import Column, Integer, String, Date
from database import Base

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True)
    email = Column(String(120), unique=True)

    def __init__(self, name=None, email=None):
        self.name = name
        self.email = email

    def __repr__(self):
        return '<User %r>' % (self.name)
        
class Task(Base):
	__tablename__ = "task"
	id_task = Column(Integer, primary_key=True)
	start_date = Column(Date)
	state = Column(Integer)
	mode = Column(Integer)
	
	def __init__(self, id_task = None, start_date = None, mode = 0):
		self.id_task = id_task
		self.start_date = start_date
		self.mode = mode
		self.state = 0
        #~ self.mode = mode
        #~ self.state = 0
        
	def __repr__(self):
		return '<Task %r>' % (self.id_task)
