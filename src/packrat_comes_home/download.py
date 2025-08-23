# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from itertools import groupby
from pathlib import Path

import sqlalchemy as sa
import yaml
from dropbox import Dropbox
from tqdm import tqdm

from .log import logger
from .schema import ConnectionManager, DeleteEvent, FileEvent, ModifyEvent, SymlinkEvent
from .utils import robust_call


def dropbox_hash(path: Path) -> str:
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


def truncate(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w"):  # empty file as placeholder
        pass


def get_snapshot_name(snap_datetime: datetime) -> str:
    microseconds = snap_datetime.microsecond
    milliseconds = microseconds // 1000
    if not milliseconds < 1000:
        raise ValueError(f"Invalid microseconds {microseconds}")

    snap_date_format = "%Y%m%d_%H%M%S"
    snap_name = f"dropbox_{snap_datetime.strftime(snap_date_format)}_{milliseconds:03d}"
    return snap_name


@dataclass
class Downloader:
    session: sa.orm.Session
    chunk_size: int = 2**8

    configuration: dict[str, str] = field(init=False)
    dbx: Dropbox = field(init=False)

    zfs_data_set: str = field(init=False)
    base_path: Path = field(init=False)

    snapshots: set[str] = field(init=False)

    is_first_snapshot: bool = field(init=False, default=True)

    def __post_init__(self) -> None:
        with open("config.yml", "r") as file_handle:
            self.configuration = yaml.load(file_handle, Loader=yaml.Loader)
        self.dbx = Dropbox(self.configuration["dropbox_token"])
        self.zfs_data_set = self.configuration["zfs_data_set"]
        self.base_path = Path(f"/{self.zfs_data_set}")
        self.snapshots = {
            snap.split("@")[-1]
            for snap in subprocess.check_output(
                [
                    "zfs",
                    "list",
                    "-H",
                    "-o",
                    "name",
                    "-t",
                    "snapshot",
                    self.zfs_data_set,
                ],
                encoding="utf-8",
            ).splitlines()
        }

    def set_mtime(
        self, path: Path, seconds: float, update_parents: bool = False
    ) -> None:
        os.utime(path, times=(seconds, seconds))
        if update_parents:  # set folder mtime on file create
            for parent in path.parents:
                if parent == self.base_path:
                    break
                os.utime(parent, times=(seconds, seconds))

    def download_file(self, file: FileEvent) -> None:
        if not isinstance(file.path, str):
            raise ValueError('File object is missing "path" attribute')
        path = self.base_path / file.path.lstrip("/")

        if not isinstance(file.timestamp, datetime):
            raise ValueError('File object is missing "timestamp" attribute')
        seconds = file.timestamp.timestamp()

        if isinstance(file, ModifyEvent):
            is_new_file = not path.is_file()

            truncate(path)

            if file.is_downloadable is True:
                logger.debug(f'Download "{path}" at "{file.revision}"')
                robust_call(
                    self.dbx.files_download_to_file,
                    path,
                    f"rev:{file.revision}",
                )

                if isinstance(file.content_hash, str):
                    if not dropbox_hash(path) == file.content_hash:
                        raise ValueError(f'"{path}" hash mismatch')

            self.set_mtime(path, seconds, update_parents=is_new_file)

        elif isinstance(file, SymlinkEvent):
            is_new_file = not path.is_file()

            truncate(path)

            path.unlink()

            if not isinstance(file.target, str):
                raise ValueError('File object is missing "target" attribute')
            target = self.base_path / file.target.lstrip("/")
            path.symlink_to(target)

        elif isinstance(file, DeleteEvent):
            if path.is_file():
                logger.debug(f'Delete "{path}"')
                path.unlink()
            elif self.is_first_snapshot:
                logger.debug(f'Already deleted "{path}"')
            else:
                logger.warning(f'Cannot delete non-existent "{path}"')

            for parent in path.parents:
                if parent == self.base_path:
                    break
                if not parent.is_dir():
                    continue

                if is_empty(parent):
                    logger.debug(f'Delete empty directory "{parent}"')
                    try:
                        parent.rmdir()
                    except FileNotFoundError:
                        pass
                else:  # update mtime on delete
                    self.set_mtime(parent, seconds)

        else:
            raise NotImplementedError(f'file.type="{file.type}"')

    def take_snapshot(self, snap_datetime: datetime) -> None:
        snap_name = get_snapshot_name(snap_datetime)
        command = ["zfs", "snapshot", f"{self.zfs_data_set}@{snap_name}"]
        logger.debug(f'Run "{" ".join(command)}"')
        subprocess.call(command)

    def download_revisions(self, file_events: list[FileEvent]) -> None:
        # input is already sorted
        seen = set()
        for i, file in enumerate(file_events):
            if file.path in seen:  # duplicate
                self.download_revisions(file_events[:i])  # split
                self.download_revisions(file_events[i:])
                return
            seen.add(file.path)

        timestamp = file_events[-1].timestamp
        snap_name = get_snapshot_name(timestamp)
        if snap_name in self.snapshots:
            logger.info(f'Skip existing snapshot "{snap_name}"')
            return

        # modify files
        logger.info(
            f"Applying {len(file_events)} revisions from "
            f"{file_events[0].timestamp} to {timestamp}"
        )
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = list()
            for file in file_events:
                future = executor.submit(self.download_file, file)
                futures.append(future)

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                unit="files",
                leave=False,
                position=1,
            ):
                exception = future.exception()
                if exception is not None:
                    raise exception

        self.take_snapshot(timestamp)
        self.is_first_snapshot = False

    def make_initial_state(self) -> None:
        (min_datetime,) = self.session.query(sa.func.min(FileEvent.timestamp)).one()
        seconds = min_datetime.timestamp()

        event_path = FileEvent.path.label("path")
        event_type = FileEvent.type.label("type")
        row_number = (
            sa.func.row_number()
            .over(order_by=FileEvent.timestamp.asc(), partition_by=FileEvent.path)
            .label("row_number")
        )
        subquery = self.session.query(event_path, event_type, row_number)
        subquery = subquery.yield_per(self.chunk_size).subquery()

        # Create empty files where we cannot restore from history
        query = (
            self.session.query(subquery.c.path)
            .filter(subquery.c.row_number == 1)
            .filter(subquery.c.type == "delete")
        )
        query = query.yield_per(self.chunk_size)
        for (path,) in tqdm(query, unit="files"):
            path = self.base_path / path.lstrip("/")
            truncate(path)
            self.set_mtime(path, seconds, update_parents=True)
        self.take_snapshot(min_datetime)


def download() -> None:
    # Setup
    connection_manager = ConnectionManager()
    session = connection_manager.make_session()
    downloader = Downloader(session)

    # Initial state
    if is_empty(downloader.base_path):
        downloader.make_initial_state()

    # modifications
    chunk_size = 1024
    query = session.query(FileEvent).order_by(FileEvent.timestamp.asc())
    query = query.yield_per(chunk_size)

    def time(file: FileEvent):
        return (
            file.timestamp.year,
            file.timestamp.month,
            file.timestamp.day,
        )

    for _, group in tqdm(groupby(query, key=time), unit="snapshots", position=0):
        downloader.download_revisions(list(group))
