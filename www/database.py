from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.interfaces
import _mysql_exceptions

from settings import Config

c = Config()

class MyListener(sqlalchemy.interfaces.PoolListener):
    def __init__(self):
       self.retried = False
    def checkout(self, dbapi_con, con_record, con_proxy):
       try:
           dbapi_con.info() # is there any better way to simply check if connection to mysql is alive?
       except sqlalchemy.exc.OperationalError:
           if self.retried:
               self.retried = False
               raise # we do nothing
           self.retried = True
           raise sqlalchemy.exc.DisconnectionError

engine = create_engine(c.DB_STRING, convert_unicode=True, listeners=[MyListener()])
db_session = scoped_session(sessionmaker(autocommit=True,
                                         autoflush=True,
                                         bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

