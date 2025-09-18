import requests
import time
import random
import os
import json
from sqlalchemy.exc import IntegrityError
from database.models import TPostalArea
from services.bot.interfaces import IBotManager
from sqlalchemy.orm import Session
from datetime import date
from services.bot.task_manager import TaskManager
from typing import Optional
from database.connection import Connection
from services.utils import config

class WorkerManager:
    def __init__(self, bot_manager: IBotManager, db_connection: Connection):
        self.bot_manager: IBotManager = bot_manager
        self.db_connection: Connection = db_connection
        self.session: Optional[Session] = None
        self.run: bool = False

    def close_session(self) -> None:
        if self.session:
            self.session.close()

    def stop(self) -> None:
        self.run = False

    def start(self) -> None:
        self.run = True
        self.session = self.db_connection.open_session()
        try:
            while self.run:
                completed_tasks = 0
                today = date.today()
                for task_manager in self.bot_manager.task_manager_list:
                    task_manager: TaskManager
                    task = task_manager.get_task()
                    if task is not None:
                        self.work(target_url=task_manager.target_url,
                                target_country=task_manager.target_country,
                                t_postal_area=task,
                                today=today)
                    else:
                        completed_tasks += 1
                        #self.bot_manager.logger(f"Completed Tasks: {completed_tasks}/{len(self.bot_manager.task_manager_list)}", force=True)
                
                if completed_tasks == len(self.bot_manager.task_manager_list):
                    break
        except Exception as e:
            self.bot_manager.logger(f"Error: {e}", force=True)
        finally:
            self.close_session()

    def work(self, target_url: str, target_country: str, t_postal_area: TPostalArea, today: date) -> None:
        t_postal_area = self.session.merge(t_postal_area)

        pa_code = t_postal_area.pa_code
        if t_postal_area.pa_status_code != 400:
            status_code_tmp = None
            try:
                self.bot_manager.logger(t_postal_area.pa_id, f"{target_url}{pa_code}")
                response = requests.get(f"{target_url}{pa_code}", proxies=self.bot_manager.proxies, headers=self.bot_manager.headers)
                
                t_postal_area.pa_status_code = response.status_code
                status_code_tmp = response.status_code

                response.raise_for_status()

                if self.bot_manager.save_json_db:
                    self.bot_manager.logger(t_postal_area.pa_id, "Saving JSON in Database")
                    t_postal_area.pa_data = json.dumps(response.json())

                self.bot_manager.logger(t_postal_area.pa_id, "Data: ", str(t_postal_area.pa_data)[0:200]+"...")
                self.session.commit()

                if self.bot_manager.save_json_file:
                    self.bot_manager.logger(t_postal_area.pa_id, "Saving JSON File")
                    folder = f"{config.JSON_LOG_DIR}/{today}/{target_country}"
                    os.makedirs(folder, exist_ok=True)

                    filename = f"{pa_code}.json"
                    filepath = os.path.join(folder, filename)

                    with open(filepath, "w", encoding="utf-8") as f:
                        json.dump(response.json(), f, ensure_ascii=False, indent=4)
                
                if self.bot_manager.transform_to_tabular:
                    self.bot_manager.logger(t_postal_area.pa_id, "Transforming JSON..")
                    self.bot_manager._tabular_transform_tr(pa_id=t_postal_area.pa_id, json_data=response.json(), log=True)
            
            except IntegrityError as e:
                # commit status code
                self.bot_manager.logger(t_postal_area.pa_id, "Duplicate")
                self.session.rollback()
                t_postal_area.pa_status_code = status_code_tmp
                self.session.commit()
            except requests.RequestException as e:
                # commit status code
                self.bot_manager.logger(t_postal_area.pa_id, "Status Code:", t_postal_area.pa_status_code)
                self.session.commit()
            except Exception as e:
                self.session.rollback()
                self.bot_manager.logger(t_postal_area.pa_id, f"Error: {e}")

            time.sleep(random.uniform(self.bot_manager.fetch_min_delay, self.bot_manager.fetch_max_delay))
        else:
            self.bot_manager.logger(t_postal_area.pa_id, pa_code, "NO DATA!")