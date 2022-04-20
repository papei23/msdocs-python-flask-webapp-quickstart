from sqlalchemy.orm import declarative_base, Session

import pandas as pd

from database import connection

from sqlalchemy import Column, Integer, Float, Date, String, Boolean, ForeignKey, and_
from sqlalchemy.orm import relationship
import sqlalchemy as db



engine = connection()
session = Session(engine)

Base = declarative_base()




class Economic_Calendar_Data(Base):
    __tablename__ = 'economic_calendar_data'
    
    index           = Column('index', Integer, primary_key=True)
    id_             = Column('id', String)
    date            = Column('date', Date)
    time            = Column('time', String)
    zone            = Column('zone', String)
    currency        = Column('currency', String)
    importance      = Column('importance', String)
    event           = Column('event', String)
    actual          = Column('actual', String)
    forecast        = Column('forecast', String)
    previous        = Column('previous', String)
    actual_clear    = Column('actual_clear', Float)
    ispercentage    = Column('ispercentage', Boolean)
    event_clear     = Column('event_clear', String)
    #children = relationship("Todays_values")
    
    @staticmethod
    def get_all(self):
        return session.query(Economic_Calendar_Data).all()
    
    @staticmethod
    def get_max_date(self):
        return session.query(db.func.max(Economic_Calendar_Data.date)).filter(Economic_Calendar_Data.actual.isnot(None)).scalar()
    
   
    def exist_row_in_db_n_update(self, date, zone, event, actual):
        row = session.query(Economic_Calendar_Data).filter(and_(Economic_Calendar_Data.date==date),
                                                            (Economic_Calendar_Data.zone==zone), 
                                                            (Economic_Calendar_Data.event==event)
                                                             ).first()
     
   
        if row is not None:
            if row.actual != actual:
                row.actual = actual
                session.commit()
             
            return True
         
        return False
    
    def insert_new_row(self, row):
        newRow = Economic_Calendar_Data( id_ = row.id
                                        ,date            = row.date
                                        ,time            = row.time
                                        ,zone            = row.zone
                                        ,currency        = row.currency
                                        ,importance      = row.importance
                                        ,event           = row.event
                                        ,actual          = row.actual
                                        ,forecast        = row.forecast
                                        ,previous        = row.previous
                                        ,actual_clear    = row.actual_clear
                                        ,ispercentage    = row.ispercentage
                                        ,event_clear     = row.event_clear

                    
                    )

        session.add(newRow) 
        session.commit()
        

  


