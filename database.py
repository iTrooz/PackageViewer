from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

Base = declarative_base()

class Package(Base):
    __tablename__ = 'package'

    id = Column(Integer, primary_key=True)

    distro_name = Column(String(255), nullable=False)
    distro_version = Column(String(255), nullable=False)
    distro_repo = Column(String(255), nullable=False)
    
    name = Column(String(255), nullable=False)
    version = Column(String(255), nullable=False)
    arch = Column(String(255), nullable=False)
    others = Column(JSON)

    def add_other(self, key, value):
        if not self.others:
            self.others = {}
        self.others[key] = value

    def __str__(self) -> str:
        return f"Package(distro={self.distro_name}-{self.distro_version}-{self.distro_repo},name={self.name})"

    def __repr__(self) -> str:
        return "Package(..)"

class PackageFile(Base):
    __tablename__ = 'package_file'

    id = Column(Integer, primary_key=True)
    filepath = Column(String(4096), nullable=False)
    package_name = Column(String(255))

db_engine = create_engine('mariadb+pymysql://root:azerty123@localhost:3306/packageviewer', echo=False, future=True)

def reset_db():
    Base.metadata.drop_all(bind=db_engine)
    Base.metadata.create_all(bind=db_engine)

def get_session():
    return Session(bind=db_engine)


def get_conn():
    return db_engine.connect().execution_options(autocommit=False)

