# Define your models
from sqlalchemy import String,Column, Integer, String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

class Base(DeclarativeBase):
    pass
class Domain(Base):
    __tablename__ = 'domains'

    id = Column(Integer, primary_key=True)
    domain = Column(String)
    title = Column(String)
    des = Column(String)
    bornat = Column(String)
    indexat = Column(String)
    indexdata = Column(String)
