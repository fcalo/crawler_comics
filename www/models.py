from sqlalchemy import Column, Integer, String, Date, Boolean, Enum
from database import Base

class Task(Base):
	__tablename__ = "task"
	id_task = Column(Integer, primary_key=True)
	start_date = Column(Date)
	type_task = Column(Integer)
	state = Column(Integer)
	mode = Column(Integer)
	week_day = Column(Integer)
	hour = Column(Integer)
	
	def __init__(self, id_task = None, start_date = None, type_task = 0, mode = 0, week_day = None, hour = None):
		self.id_task = id_task
		self.start_date = start_date
		self.type_task = type_task
		self.mode = mode
		self.state = 0
		self.week_day = week_day
		self.hour = hour
		
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
		
		
class Newsletter(Base):
	__tablename__ = "newsletter"
	id_newsletter = Column(Integer, primary_key = True, autoincrement = True)
	header_text = Column(String)
	banner_1_active = Column(Boolean)
	banner_1_url = Column(String)
	banner_1_image = Column(String)
	banner_2_active = Column(Boolean)
	banner_2_url = Column(String)
	banner_2_image = Column(String)
	banner_3_active = Column(Boolean)
	banner_3_url = Column(String)
	banner_3_image = Column(String)
	banner_4_active = Column(Boolean)
	banner_4_url = Column(String)
	banner_4_image = Column(String)
	type_link = Column(Enum("N", "A"))
	id_affil = Column(Integer)
	template = Column(Enum("N", "R"))
	date_from = Column(Date)
	date_to = Column(Date)
	state = Column(Integer)
	
	def __init__(self, id_newsletter = None, header_text = None, 
	  banner_1_active = None, banner_1_url = None, banner_1_image = None,
	  banner_2_active = None, banner_2_url = None, banner_2_image = None,
	  banner_3_active = None, banner_3_url = None, banner_3_image = None,
	  banner_4_active = None, banner_4_url = None, banner_4_image = None,
	  type_link = None, id_affil = None, template = None, 
	  date_from = None, date_to = None):
		self.id_newsletter = id_newsletter
		self.header_text = header_text
		self.banner_1_active = banner_1_active
		self.banner_1_url = banner_1_url
		self.banner_1_image = banner_1_image
		self.banner_2_active = banner_2_active
		self.banner_2_url = banner_2_url
		self.banner_2_image = banner_2_image
		self.banner_3_active = banner_3_active
		self.banner_3_url = banner_3_url
		self.banner_3_image = banner_3_image
		self.banner_4_active = banner_4_active
		self.banner_4_url = banner_4_url
		self.banner_4_image = banner_4_image
		self.type_link = type_link
		self.id_affil = id_affil
		self.template = template
		self.date_from = date_from
		self.date_to = date_to
		self.state = 0
        
	def __repr__(self):
		return self.id_newsletter
	
	
class Category(Base):
	__tablename__ = "category"
	category = Column(String, primary_key=True)
	
	def __init__(self, category = None):
		self.category = category
        
	def __repr__(self):
		return self.category

class NewsletterCategory(Base):
	__tablename__ = "newsletter_category"
	id_newsletter = Column(Integer, primary_key = True)
	category = Column(String, primary_key=True)
	
	def __init__(self, id_newsletter, category = None):
		self.id_newsletter = id_newsletter
		self.category = category
        
	def __repr__(self):
		return self.category
