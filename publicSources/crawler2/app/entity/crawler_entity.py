from sqlalchemy import Column, Integer, String, Boolean, Text, func, DateTime

#from app.config.sql_lite_db_configuration import Base


class Crawler():
    __tablename__ = "url_configuration"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(Text)