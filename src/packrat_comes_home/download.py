# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
from hashlib import sha256
from itertools import groupby
from pathlib import Path
from time import sleep

import sqlalchemy as sa
import yaml
from dropbox import Dropbox
from requests import ConnectionError, ReadTimeout

from .log import logger
from .schema import ConnectionManager, DeleteEvent, FileEvent, ModifyEvent, SymlinkEvent

chunk_size = 2**8
connection_manager = ConnectionManager()

with open("config.yml", "r") as file_handle:
    configuration = yaml.load(file_handle, Loader=yaml.Loader)

dbx = Dropbox(configuration["dropbox_token"])

zfs_data_set = configuration["zfs_data_set"]
base_path = Path(f"/{zfs_data_set}")


def dropbox_hash(path: Path):
    """

    Based on https://www.dropbox.com/developers/reference/content-hash

    """
    block_hashes = b""
    with open(path, "rb") as file_handle:
        while chunk := file_handle.read(4194304):
            block_hashes += sha256(chunk).digest()
    return sha256(block_hashes).hexdigest()


def is_empty(path: Path) -> bool:
    try:
        for child in path.iterdir():
            if child.name == ".zfs":  # not a real file
                continue
            return False
    except (FileNotFoundError, StopIteration):
        pass

    return True


def set_mtime(path: Path, seconds: float, update_parents: bool = False):
    os.utime(path, times=(seconds, seconds))
    if update_parents:  # set folder mtime on file create
        for parent in path.parents:
            if parent == base_path:
                break
            os.utime(parent, times=(seconds, seconds))


def truncate(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w"):  # empty file as placeholder
        pass


def robust_call(func, *args, **kwargs):
    while True:
        try:
            return func(*args, **kwargs)
        except (ReadTimeout, ConnectionError) as e:
            logger.error("Network error %s", exc_info=e)
            sleep(1e1)


def download_file(file: FileEvent):
    if not isinstance(file.path, str):
        raise ValueError('File object is missing "path" attribute')
    path = base_path / file.path.lstrip("/")

    if not isinstance(file.timestamp, datetime):
        raise ValueError('File object is missing "timestamp" attribute')
    seconds = file.timestamp.timestamp()

    if isinstance(file, ModifyEvent):
        is_new_file = not path.is_file()

        truncate(path)

        if file.is_downloadable is True:
            logger.info(f'Download "{path}" at "{file.revision}"')
            robust_call(
                dbx.files_download_to_file,
                path,
                f"rev:{file.revision}",
            )

            if isinstance(file.content_hash, str):
                if not dropbox_hash(path) == file.content_hash:
                    raise ValueError(f'"{path}" hash mismatch')

        set_mtime(path, seconds, update_parents=is_new_file)

    elif isinstance(file, SymlinkEvent):
        is_new_file = not path.is_file()

        truncate(path)

        path.unlink()

        if not isinstance(file.target, str):
            raise ValueError('File object is missing "target" attribute')
        target = base_path / file.target.lstrip("/")
        path.symlink_to(target)

    elif isinstance(file, DeleteEvent):
        if path.is_file():
            logger.debug(f'Delete "{path}"')
            path.unlink()
        else:
            logger.warning(f'Cannot delete non-existent "{path}"')

        for parent in path.parents:
            if parent == base_path:
                break
            if not parent.is_dir():
                continue

            if is_empty(parent):
                logger.debug(f'Delete empty directory "{parent}"')
                parent.rmdir()
            else:  # update mtime on delete
                set_mtime(parent, seconds)

    else:
        raise NotImplementedError(f'file.type="{file.type}"')


def take_snapshot(snap_datetime: datetime):
    microseconds = snap_datetime.microsecond
    milliseconds = microseconds // 1000
    if not milliseconds < 1000:
        raise ValueError(f"Invalid microseconds {microseconds}")

    snap_date_format = "%Y%m%d_%H%M%S"
    snap_name = f"dbx_{snap_datetime.strftime(snap_date_format)}_{milliseconds:03d}"

    command = ["zfs", "snapshot", f"{zfs_data_set}@{snap_name}"]
    logger.info(f'Run "{" ".join(command)}"')
    subprocess.call(command)


def download_revisions(file_events: list[FileEvent]):
    # input is already sorted
    seen = set()
    for i, file in enumerate(file_events):
        if file.path in seen:  # duplicate
            download_revisions(file_events[:i])  # split
            download_revisions(file_events[i:])
            return
        seen.add(file.path)

    # modify files
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = list()
        for file in file_events:
            futures.append(executor.submit(download_file, file))

        wait(futures)

    take_snapshot(file_events[-1].timestamp)


def download():
    # setup
    session = connection_manager.make_session()
    chunk_size = 1024

    # initial state
    if not is_empty(base_path):
        raise ValueError(f'"{base_path}" is not empty')
    (min_datetime,) = session.query(sa.func.min(FileEvent.timestamp)).one()
    seconds = min_datetime.timestamp()

    path = FileEvent.path.label("path")
    type = FileEvent.type.label("type")
    row_number = (
        sa.func.row_number()
        .over(order_by=FileEvent.timestamp.asc(), partition_by=FileEvent.path)
        .label("row_number")
    )

    subquery = session.query(path, type, row_number).subquery()

    query = (
        session.query(subquery.c.path)
        .filter(subquery.c.row_number == 1)
        .filter(subquery.c.type == "delete")
    )

    query = query.yield_per(chunk_size)

    for (path,) in query:
        path = base_path / path.lstrip("/")

        truncate(path)
        set_mtime(path, seconds, update_parents=True)

    take_snapshot(min_datetime)

    # modifications
    query = session.query(FileEvent).order_by(FileEvent.timestamp.asc())
    query = query.yield_per(chunk_size)

    def day(file: FileEvent):
        return (
            file.timestamp.year,
            file.timestamp.month,
            file.timestamp.day,
        )

    for _, group in groupby(query, key=day):
        download_revisions(list(group))
