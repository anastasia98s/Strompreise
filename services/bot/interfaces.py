from abc import ABC, abstractmethod
from typing import List, Callable
from sqlalchemy.orm import Session
from database.connection import Connection

class IBotManager(ABC):
    db_connectionn: Connection
    session: Session
    task_manager_list: List
    worker_manager_list: List
    save_json_file: bool
    save_json_db: bool
    transform_to_tabular: bool
    fetch_min_delay: int
    fetch_max_delay: int
    proxies: bool | None
    headers: dict[str, str]
    logger: Callable[..., None]

    @abstractmethod
    def task_manager_init(self):
        pass

    @abstractmethod
    def add_worker(self):
        pass

    @abstractmethod
    def _tabular_transform_tr(self):
        pass