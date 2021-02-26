from sqlalchemy import Column, Integer, Numeric, String, ForeignKey, Table
from sqlalchemy.orm import relationship

from db.base import Base

# files_nuc_data_association = Table('files_nuc_data_association', Base.metadata,
#                                    Column('file_id', Integer, ForeignKey('files.id')),
#                                    Column('nuc_data_id', Integer, ForeignKey('nuc_data.id'))
#                                    )

files_physical_quantities_association = Table('files_physical_quantities_association', Base.metadata,
                                              Column('file_id', Integer, ForeignKey('files.id')),
                                              Column('physical_quantity_id', Integer,
                                                     ForeignKey('physical_quantities.id'))
                                              )

nuc_data_physical_quantities_association = Table('nuc_data_physical_quantities_association', Base.metadata,
                                                 Column('nuc_data_id', Integer, ForeignKey('nuc_data.id')),
                                                 Column('physical_quantity_id', Integer,
                                                        ForeignKey('physical_quantities.id'))
                                                 )


class Nuc(Base):
    __tablename__ = 'nuc'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    data = relationship("NucData", backref='nuc')


class NucData(Base):
    __tablename__ = 'nuc_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    nuc_id = Column(Integer, ForeignKey('nuc.id'))
    file_id = Column(Integer, ForeignKey('files.id'))
    data1 = Column(Numeric)
    data2 = Column(Numeric)

    # files = relationship('File', secondary=files_nuc_data_association)
    physical_quantities = relationship('PhysicalQuantity', secondary=nuc_data_physical_quantities_association)


class File(Base):
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    data = relationship("NucData", backref='files')

    # data = relationship('NucData', secondary=files_nuc_data_association)
    physical_quantities = relationship('PhysicalQuantity', secondary=files_physical_quantities_association)


class PhysicalQuantity(Base):
    __tablename__ = 'physical_quantities'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)

    data = relationship('NucData', secondary=nuc_data_physical_quantities_association)
    files = relationship('File', secondary=files_physical_quantities_association)
