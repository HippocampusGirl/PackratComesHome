# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

from datetime import datetime

from sqlalchemy import String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from sqlalchemy.pool import NullPool


class Base(DeclarativeBase):
    pass


class FileEvent(Base):
    __tablename__ = "file_events"
    __mapper_args__ = dict(
        polymorphic_on="type",
        polymorphic_identity="file_event",
    )

    path: Mapped[str] = mapped_column(primary_key=True)
    revision: Mapped[str | None] = mapped_column(primary_key=True)

    timestamp: Mapped[datetime] = mapped_column(index=True)

    type: Mapped[str]
    is_downloadable: Mapped[bool]
    is_deleted: Mapped[bool]


class ModifyEvent(FileEvent):
    __mapper_args__ = dict(
        polymorphic_identity="modify",
    )
    size: Mapped[int] = mapped_column(nullable=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=True)


class DeleteEvent(FileEvent):
    __mapper_args__ = dict(
        polymorphic_identity="delete",
    )


class SymlinkEvent(FileEvent):
    __mapper_args__ = dict(
        polymorphic_identity="symlink",
    )
    target: Mapped[str] = mapped_column(nullable=True)


class FileError(Base):
    __tablename__ = "file_errors"

    path: Mapped[str] = mapped_column(primary_key=True)
    message: Mapped[str | None]


class ConnectionManager:
    def __init__(self, url: str = "sqlite:///packrat.sqlite") -> None:
        self.engine = create_engine(url, poolclass=NullPool)
        self.engine.connect()

        Base.metadata.create_all(self.engine)
        self.sessionmaker = sessionmaker(bind=self.engine)

    def make_session(self) -> Session:
        return self.sessionmaker()
