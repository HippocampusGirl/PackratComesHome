from sqlalchemy import create_engine, Column, String, DateTime, Boolean, Integer
from sqlalchemy import orm
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class FileEvent(Base):
    __tablename__ = "file_events"

    path = Column(String, primary_key=True)
    revision = Column(String, primary_key=True, nullable=True)

    type = Column(String, nullable=False)

    timestamp = Column(DateTime, nullable=False, index=True)

    is_downloadable = Column(Boolean, nullable=False)
    is_deleted = Column(Boolean, nullable=False)

    __mapper_args__ =  dict(
        polymorphic_on="type",
        polymorphic_identity="file_event",
    )


class ModifyEvent(FileEvent):
    size = Column(Integer, nullable=True)
    content_hash = Column(String(64), nullable=True)

    __mapper_args__ =  dict(
        polymorphic_identity="modify",
    )


class DeleteEvent(FileEvent):
    __mapper_args__ =  dict(
        polymorphic_identity="delete",
    )


class SymlinkEvent(FileEvent):
    target = Column(String)

    __mapper_args__ =  dict(
        polymorphic_identity="symlink",
    )


class FileError(Base):
    __tablename__ = "file_errors"

    path = Column(String, primary_key=True)

    message = Column(String, nullable=True)


class ConnectionManager:

    def __init__(self) -> None:
        self.engine = create_engine("sqlite:///packrat.db", poolclass=NullPool)
        self.engine.connect()

        Base.metadata.create_all(self.engine)
        self.sessionmaker = orm.sessionmaker(bind=self.engine)

    def make_session(self) -> orm.Session:
        return self.sessionmaker()
