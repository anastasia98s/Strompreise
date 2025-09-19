from services.table_manager import TableManager
from services.csv_manager import CSVManager
from services.bot.task_manager import TaskManager
from services.bot.worker_manager import WorkerManager
from services.utils import config
from database.connection import Connection
from typing import List, Callable, Dict, Any
from services.bot.interfaces import IBotManager
import threading

class BotManager(TableManager, CSVManager, IBotManager):
    def __init__(self, db_connection: Connection, logger: Callable[..., None]):
        super().__init__(db_connection=db_connection, logger=logger)
        self.db_connection: Connection = db_connection
        self._tabular_transform_init()

        self.task_manager_list: List[TaskManager] = []
        self.worker_manager_list: List[WorkerManager] = []
        self.save_json_file: bool = True
        self.save_json_db: bool = False
        self.transform_to_tabular: bool = True
        self.fetch_min_delay: int = config.FETCH_MIN_DELAY
        self.fetch_max_delay: int = config.FETCH_MAX_DELAY
        self.proxies: bool | None = config.PROXIES if config.USE_PROXY else None
        self.headers: dict[str, str] = config.FETCH_HEADER
        self.logger: Callable[..., None] = logger
        self.in_process: bool = False
        
    def task_manager_init(self) -> None:
        if not self.in_process:
            self.task_manager_list = []
            for country in config.COUNTRY_CONFIG:
                self.add_task(country_config=country)
            self.logger("Tasks set", force=True)
        else:
            self.logger("Failed to reset tasks! Stop the process first before resetting tasks.", force=True)

    def add_task(self, country_config: Dict[str, Any]) -> None:
        task_manager = TaskManager(db_connection=self.db_connection, target_country=country_config['name'], target_url=country_config['url'], logger=self.logger)
        task_manager.set_task()
        self.task_manager_list.append(task_manager)
        self.logger(f"Task for {country_config['name']} added", force=True)

    def add_worker(self) -> None:
        self.worker_manager_list.append(WorkerManager(bot_manager=self, db_connection=self.db_connection))
        msg = "1 worker added"
        if self.in_process:
            msg += " | restart process required for effect"

        self.logger(msg, force=True)

    def remove_task(self, country_name: str) -> bool:
        for i, task in enumerate(self.task_manager_list):
            if getattr(task, 'target_country', None) == country_name:
                del self.task_manager_list[i]
                self.logger(f"Task for {country_name} deleted", force=True)
                return True
        self.logger(f"Task for {country_name} not found", force=True)
        return False
    
    def remove_worker(self) -> None:
        if self.worker_manager_list:
            worker_manager = self.worker_manager_list[-1]
            worker_manager.stop()
            self.worker_manager_list.pop(-1)
            self.logger(f"1 worker removed", force=True)
        else:
            self.logger(f"No workers", force=True)

    def stop_workers(self) -> None:
        for worker_manager in self.worker_manager_list:
            worker_manager.stop()
        self.logger(f"Workers forced to stop", force=True)

    def get_set_process(self, status=None, target=None):
        if status is not None:
            self.in_process = status
        
        message = "Process is running!" if self.in_process else "No process running!"
        self.logger(message, force=True, target=target)
        self.logger(self.in_process, force=True, action='get_process', target=target, raw=True)

    def run_workers(self) -> None:
        if self.task_manager_list:
            if self.worker_manager_list:
                if not self.in_process:
                    self.get_set_process(status=True)
                    threads = []
                    for worker_manager in self.worker_manager_list:
                        t = threading.Thread(target=worker_manager.start)
                        t.start()
                        threads.append(t)

                    for t in threads:
                        t.join()
                    self.get_set_process(status=False)
                else:
                    self.logger("the previous process is still running!", force=True)
            else:
                self.logger("no workers available!!", force=True)
        else:
            self.logger("no tasks available!!", force=True)