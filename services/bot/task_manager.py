from database.models import TCountry, TProvince, TCity, TPostalArea
from sqlalchemy import or_
from typing import List, Optional, Callable
import threading
from database.connection import Connection

class TaskManager:
    def __init__(self, db_connection: Connection, target_country: str, target_url: str, logger: Callable[..., None]):
        self.db_connection: Connection = db_connection
        self.target_country: str = target_country
        self.target_url: str = target_url
        self.task_list: List[TPostalArea] = []
        self.index: int = 0
        self.logger: Callable[..., None] = logger
        self._lock: threading.Lock = threading.Lock()
    
    def info(self, target=None) -> None:
        self.logger({"id":self.target_country, "data":f"{self.index}/{len(self.task_list)}"}, force=True, action='get_num_task', target=target, raw=True)

    def set_task(self) -> None:
        try:
            with self.db_connection.get_session() as session:   
                self.task_list = (
                    session.query(TPostalArea)
                    .select_from(TPostalArea)
                    .join(TCity, TCity.ci_id == TPostalArea.ci_id)
                    .join(TProvince, TProvince.p_id == TCity.p_id)
                    .join(TCountry, TCountry.c_id == TProvince.c_id)
                    .filter(
                        or_(
                            TPostalArea.pa_status_code != 200,
                            TPostalArea.pa_status_code.is_(None)
                        ),
                        TCountry.c_name == self.target_country
                    )
                    .all()
                )
                self.index = 0
        except Exception as e:
            self.logger(f"Error: {e}", force=True)
        self.info()

    def get_task(self) -> Optional[TPostalArea]:
        with self._lock:
            self.info()
            if self.index < len(self.task_list):
                task = self.task_list[self.index]
                self.index += 1
                return task
            else:
                return None