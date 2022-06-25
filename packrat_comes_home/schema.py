from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import create_engine, Column, String, DateTime, Boolean, Integer
from sqlalchemy import orm
from sqlalchemy.pool import NullPool

mapper_registry: orm.registry = orm.registry()


@mapper_registry.mapped
@dataclass
class FileEvent:
    __tablename__ = "file_events"

    path: str = Column(String, primary_key=True)
    revision: str | None = Column(String, primary_key=True, nullable=True)

    type: str = Column(String, nullable=False)

    timestamp: datetime = Column(DateTime, nullable=False, index=True)

    is_downloadable: bool = Column(Boolean, nullable=False)
    is_deleted: bool = Column(Boolean, nullable=False)

    __mapper_args__ = dict(
        polymorphic_on="type",
        polymorphic_identity="file_event",
    )


@mapper_registry.mapped
@dataclass
class ModifyEvent(FileEvent):
    size: int | None = Column(Integer, nullable=True)
    content_hash: str | None = Column(String(64), nullable=True)

    __mapper_args__ = dict(
        polymorphic_identity="modify",
    )


@mapper_registry.mapped
@dataclass
class DeleteEvent(FileEvent):
    __mapper_args__ = dict(
        polymorphic_identity="delete",
    )


@mapper_registry.mapped
@dataclass
class SymlinkEvent(FileEvent):
    target: str = Column(String)

    __mapper_args__ = dict(
        polymorphic_identity="symlink",
    )


@mapper_registry.mapped
@dataclass
class FileError:
    __tablename__ = "file_errors"

    path: str = Column(String, primary_key=True)

    message: str | None = Column(String, nullable=True)


class ConnectionManager:
    def __init__(self, url: str = "sqlite:///packrat.db") -> None:
        self.engine = create_engine(url, poolclass=NullPool)
        self.engine.connect()

        mapper_registry.metadata.create_all(self.engine)
        self.sessionmaker = orm.sessionmaker(bind=self.engine)

    def make_session(self) -> orm.Session:
        return self.sessionmaker()
