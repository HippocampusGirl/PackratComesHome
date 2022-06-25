#!/usr/bin/env python

from typing import Generator

from logging import warning, error, basicConfig as logging_basic_config
from time import sleep
import yaml

from itertools import chain
from more_itertools import ichunked
from tqdm import tqdm

from dropbox import Dropbox
from dropbox.exceptions import ApiError
from dropbox.files import (
    ListFolderResult,
    FolderMetadata,
    FileMetadata,
    DeletedMetadata,
    ListRevisionsResult,
    SymlinkInfo,
)
from requests import ReadTimeout, ConnectionError

from sqlalchemy.orm import Session
from sqlalchemy.sql import exists

from schema import (
    FileEvent,
    ModifyEvent,
    DeleteEvent,
    FileError,
    ConnectionManager,
    SymlinkEvent,
)

logging_basic_config(filename="populate.log")

chunk_size = 2**8
connection_manager = ConnectionManager()

with open("config.yml", "r") as file_handle:
    configuration = yaml.load(file_handle, Loader=yaml.Loader)

dbx = Dropbox(configuration["dropbox_token"])


def robust_call(func, *args, **kwargs):
    while True:
        try:
            return func(*args, **kwargs)
        except (ReadTimeout, ConnectionError) as e:
            error("Network error %s", exc_info=e)
            sleep(1e1)


def list_recursive() -> Generator[
    FileMetadata | FolderMetadata | DeletedMetadata, None, None
]:
    list_folder_result = robust_call(
        dbx.files_list_folder,
        "",
        include_deleted=True,
        include_mounted_folders=True,
        recursive=True,
    )
    assert isinstance(list_folder_result, ListFolderResult)

    while list_folder_result.has_more is True:

        yield from list_folder_result.entries

        list_folder_result = robust_call(
            dbx.files_list_folder_continue,
            list_folder_result.cursor,
        )
        assert isinstance(list_folder_result, ListFolderResult)


def list_revisions(
    m: FileMetadata | FolderMetadata | DeletedMetadata,
) -> Generator[FileEvent | FileError, None, None]:
    if isinstance(m, FolderMetadata):
        return

    path: str = m.path_display

    try:
        list_revisions_result = robust_call(dbx.files_list_revisions, path)
    except ApiError as e:
        yield FileError(path=path, message=repr(e))
        return

    if not isinstance(list_revisions_result, ListRevisionsResult):
        warning("Cannot handle list_revisions_result %s", list_revisions_result)
        return

    revisions = list_revisions_result.entries

    for r in revisions:

        if not isinstance(r, FileMetadata):
            warning("Cannot handle revision %s", r)
            continue

        file_event_kwargs = dict(
            path=path,
            revision=r.rev,
            is_deleted=False,
            is_downloadable=r.is_downloadable,
            timestamp=r.server_modified,
        )

        symlink_info = getattr(r, "symlink_info", None)
        if isinstance(symlink_info, SymlinkInfo):
            yield SymlinkEvent(
                **file_event_kwargs,
                target=symlink_info.target,
            )
            continue

        yield ModifyEvent(
            **file_event_kwargs,
            size=r.size,
            content_hash=r.content_hash,
        )

    if list_revisions_result.is_deleted is True:
        yield DeleteEvent(
            path=path,
            is_deleted=True,
            is_downloadable=False,
            timestamp=list_revisions_result.server_deleted,
        )


def check_path_unseen(session: Session, p: str) -> bool:
    file_event_exists = session.query(exists().where(FileEvent.path == p)).scalar()

    file_error_exists = session.query(exists().where(FileError.path == p)).scalar()

    return (file_event_exists is not True) and (file_error_exists is not True)


for m_iter in tqdm(ichunked(list_recursive(), chunk_size), unit="chunks"):
    session = connection_manager.make_session()

    session.add_all(
        chain.from_iterable(
            list_revisions(m)
            for m in tqdm(m_iter, total=chunk_size, unit="paths", leave=False)
            if check_path_unseen(session, m.path_display)
        )
    )

    session.commit()
