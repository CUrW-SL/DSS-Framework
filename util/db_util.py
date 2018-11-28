from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, exists
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import Column, VARCHAR, DATETIME, DECIMAL, INT
from util.logger_util import get_logger

MYSQL_HOST = 'localhost'
MYSQL_PORT = '3306'
MYSQL_USER = 'dss'
MYSQL_PASSWORD = 'dss12345'
MYSQL_DB = 'dss'

Base = declarative_base()
connection_string = 'mysql://{}:{}@{}:{}/{}'.format(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT, MYSQL_DB)
engine = create_engine(connection_string, echo=False)
db_util_logger = get_logger('db_util')


class ModelInfo(Base):
    __tablename__ = 'tbl_model_info'
    model_id = Column(INT, primary_key=True)
    model_type = Column(VARCHAR(20), nullable=True)
    model_name = Column(VARCHAR(30), nullable=True)
    model_config = Column(VARCHAR(500), nullable=True)

    # TODO define foreign keys

    def __init__(self, **kwrgs):
        if 'model_type' in kwrgs:
            self.model_type = kwrgs['model_type']
        if 'model_name' in kwrgs:
            self.model_name = kwrgs['model_name']
        if 'model_config' in kwrgs:
            self.model_config = kwrgs['model_config']


class ModelTask(Base):
    __tablename__ = 'tbl_model_taks'
    task_id = Column(INT, primary_key=True)
    start_datetime = Column(DATETIME, nullable=False)
    end_datetime = Column(DATETIME, nullable=False)
    model_id = Column(INT, nullable=True)

    # TODO define foreign keys

    def __init__(self, **kwrgs):
        if 'task_id' in kwrgs:
            self.task_id = kwrgs['task_id']
        if 'start_datetime' in kwrgs:
            self.start_datetime = kwrgs['start_datetime']
        if 'end_datetime' in kwrgs:
            self.end_datetime = kwrgs['end_datetime']
        if 'model_id' in kwrgs:
            self.model_id = kwrgs['model_id']


class DssUnit(Base):
    __tablename__ = 'tbl_dss_unit'
    run_id = Column(INT, primary_key=True)
    input_task_id = Column(INT, nullable=False)
    output_task_id = Column(INT, nullable=False)

    # TODO define foreign keys

    def __init__(self, **kwrgs):
        if 'run_id' in kwrgs:
            self.run_id = kwrgs['run_id']
        if 'input_task_id' in kwrgs:
            self.input_task_id = kwrgs['input_task_id']
        if 'output_task_id' in kwrgs:
            self.output_task_id = kwrgs['output_task_id']


class DssDb:
    def __init__(self):
        self.Session = sessionmaker(bind=engine)

    def insert_model_config(self, **kwargs):
        try:
            session = self.Session()
            model_info = ModelInfo(**kwargs)
            session.add(model_info)
            session.commit()
        finally:
            session.close()
            db_util_logger.info('successfully inserted model config.')

    def insert_model_task(self, **kwargs):
        try:
            session = self.Session()
            model_task = ModelTask(**kwargs)
            session.add(model_task)
            session.commit()
        finally:
            session.close()
            db_util_logger.info('successfully inserted model task.')

    def insert_dss_unit(self, **kwargs):
        try:
            session = self.Session()
            dss_unit = DssUnit(**kwargs)
            session.add(dss_unit)
            session.commit()
        finally:
            session.close()
            db_util_logger.info('successfully inserted dss unit.')


if __name__ == '__main__':
    print('')
    dss_db = DssDb()
    dss_db.insert_model_config(model_type='HecHms17723', model_name='HecHms_Distributed')


