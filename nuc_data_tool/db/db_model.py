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
      ┌────┴─────┐           ┌─────────┐
      │          │many    one│         │
      │ nuc_data ├──────────►│  files  │
      │          │           │         │
      └────┬─────┘           └────┬────┘
           │many                  │many
           │                      │
           │                      │
           ▼ one                  │
┌───────────────────────┐         │
│                       │         │
│  physical_quantities  ├─────────┘
│                       │many
└───────────────────────┘

"""

from sqlalchemy import Column, Integer, Numeric, String, LargeBinary, ForeignKey, Table
from sqlalchemy.orm import relationship

from nuc_data_tool.db.base import Base

files_physical_quantities_association = Table('files_physical_quantities_association', Base.metadata,
                                              Column('file_id', Integer, ForeignKey('files.id')),
                                              Column('physical_quantity_id', Integer,
                                                     ForeignKey('physical_quantities.id'))
                                              )


class Nuc(Base):
    __tablename__ = 'nuc'
    id = Column(Integer, primary_key=True)
    nuc_ix = Column(Integer, unique=True, autoincrement=True)
    name = Column(String(32))

    data = relationship('NucData', back_populates='nuc')


class NucData(Base):
    __tablename__ = 'nuc_data'
    id = Column(Integer, primary_key=True)
    nuc_id = Column(Integer, ForeignKey('nuc.id'))
    file_id = Column(Integer, ForeignKey('files.id'))
    physical_quantity_id = Column(Integer, ForeignKey('physical_quantities.id'))
    first_step = Column(Numeric(25))
    last_step = Column(Numeric(25))
    middle_steps = Column(LargeBinary)

    nuc = relationship('Nuc', back_populates='data')
    file = relationship('File', back_populates='data')
    physical_quantity = relationship('PhysicalQuantity', back_populates='data')


class File(Base):
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50))

    data = relationship('NucData', back_populates='file')
    physical_quantities = relationship('PhysicalQuantity',
                                       secondary=files_physical_quantities_association,
                                       back_populates='files')


class PhysicalQuantity(Base):
    __tablename__ = 'physical_quantities'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(16))

    data = relationship('NucData', back_populates='physical_quantity')
    files = relationship('File', secondary=files_physical_quantities_association,
                         back_populates='physical_quantities')
