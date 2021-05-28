"""
Database model

       ┌───────┐
       │       │
       │  nuc  │
       │       │
       └───────┘
           ▲ one
           │
           │
           │many
      ┌────┴─────┐           ┌────────┐
      │          │many    one│        │
      │ nuc_data ├──────────►│  file  │
      │          │           │        │
      └────┬─────┘           └────┬───┘
           │many                  │many
           │                      │
           │                      │
           ▼ one                  │
┌─────────────────────┐           │
│                     │           │
│  physical_quantity  ├───────────┘
│                     │  many
└─────────────────────┘

"""

from sqlalchemy import Column, Integer, Numeric, String, LargeBinary, Interval, Boolean, ForeignKey, Table
from sqlalchemy.orm import relationship

from nuc_data_tool.db.base import Base

file_physical_quantity_association = Table('file_physical_quantity_association', Base.metadata,
                                              Column('file_id', Integer, ForeignKey('files.id'), nullable=False),
                                              Column('physical_quantity_id', Integer,
                                                     ForeignKey('physical_quantity.id'), nullable=False)
                                              )


class Nuc(Base):
    __tablename__ = 'nuc'
    id = Column(Integer, primary_key=True)
    nuc_ix = Column(Integer, unique=True, autoincrement=True, nullable=False)
    name = Column(String(32), nullable=False)

    data = relationship('NucData', back_populates='nuc')


class NucData(Base):
    __tablename__ = 'nuc_data'
    id = Column(Integer, primary_key=True)
    nuc_id = Column(Integer, ForeignKey('nuc.id'), nullable=False)
    file_id = Column(Integer, ForeignKey('files.id'), nullable=False)
    physical_quantity_id = Column(Integer, ForeignKey('physical_quantity.id'), nullable=False)
    first_step = Column(Numeric(25), nullable=False)
    last_step = Column(Numeric(25), nullable=False)
    middle_steps = Column(LargeBinary)

    nuc = relationship('Nuc', back_populates='data')
    file = relationship('File', back_populates='data')
    physical_quantity = relationship('PhysicalQuantity', back_populates='data')


class File(Base):
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)
    time_interval = Column(Interval)
    repeat_times = Column(Integer)
    is_all_step = Column(Boolean)

    data = relationship('NucData', back_populates='file')
    physical_quantity = relationship('PhysicalQuantity',
                                       secondary=file_physical_quantity_association,
                                       back_populates='files')


class PhysicalQuantity(Base):
    __tablename__ = 'physical_quantity'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(16), nullable=False)

    data = relationship('NucData', back_populates='physical_quantity')
    files = relationship('File', secondary=file_physical_quantity_association,
                         back_populates='physical_quantity')
