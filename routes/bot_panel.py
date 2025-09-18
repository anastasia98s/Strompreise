from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import text
from fastapi import Depends
import inspect
from services.bot.bot_manager import BotManager
from database.connection import Connection
import asyncio
import threading
from services.utils import config

class BotPanelAPI:
    def __init__(self):
        self.router = APIRouter(prefix="/api/bot_panel", tags=["Bot Panel"])
        self.db_connection: Connection = Connection(db_hostname="mssql")
        self.user_connections = []
        self.timer = 0
        self.verbose_log = False
        self.logger_loop = None
        self.pause_timer = False
        self.scheduler_interval = config.SCHEDULER_INTERVAL-1
        self.bot_manager: BotManager = None
        self._init()
    
    def logger(self, *args: object, force=False, action=None, target=None, raw=False) -> None:
        if raw:
            args_str = args[0] if len(args) > 0 else None
        else:
            args_str = " ".join(str(arg) for arg in args) if len(args) > 0 else None

        message_json = None
        if args_str is not None:
            if self.verbose_log or force:
                if action:
                    message_json = {"action": action, "data": args_str}
                else:
                    message_json = {"action": "get_log", "data": args_str}
        
        if message_json:
            if self.logger_loop and not self.logger_loop.is_closed():
                asyncio.run_coroutine_threadsafe(self._send_message(message_json, target_ws=target), self.logger_loop)

    def _init(self):
        @self.router.websocket("/bot_panel_ws")
        async def websocket_endpoint(websocket: WebSocket):
            await self.bot_panel_ws(websocket)

        def setup_logger_loop():
            def run_loop():
                self.logger_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.logger_loop)
                self.logger_loop.run_forever()
            
            threading.Thread(target=run_loop, daemon=True).start()
            
            while self.logger_loop is None:
                threading.Event().wait(0.01)

        async def background_job():
            while True:
                await self._send_message({"action": "get_timer", "data": self.timer})
                if not self.pause_timer:
                    if self.timer > 0:
                        self.timer -= 1
                    else:
                        self._reset_timer()
                        self.run_scheduler(new_session=True)

                await asyncio.sleep(1)

        setup_logger_loop()
        self.bot_manager = BotManager(db_connection=self.db_connection, logger=self.logger)
        asyncio.create_task(background_job())

    def run_scheduler(self, new_session=False):
        def run():
            if self.bot_manager.worker_manager_list:
                if not self.bot_manager.in_process and new_session:
                    self.bot_manager.clear_bot_data_session()
                self.bot_manager.task_manager_init()
                self.bot_manager.run_workers()
            else:
                self.logger("no workers available!", force=True)

        threading.Thread(target=run).start()

    def _reset_timer(self):
        self.timer = self.scheduler_interval

    async def _send_message(self, message, target_ws=None):
        async def _broadcast_message():
            disconnected = []
            for websocket in self.user_connections:
                try:
                    await websocket.send_json(message)
                except Exception as e:
                    print(f"Error sending to websocket: {e}")
                    disconnected.append(websocket)
            
            for ws in disconnected:
                try:
                    self.user_connections.remove(ws)
                except ValueError:
                    pass
        
        async def _private_message():
            await target_ws.send_json(message)

        if target_ws:
            await _private_message()
        else:
            await _broadcast_message()

    async def bot_panel_ws(self, websocket: WebSocket):
        action_map = {
            "set_scheduler_interval": self._set_scheduler_interval,
            "remove_worker": self._remove_worker,
            "add_worker": self._add_worker,
            "set_process": self._set_process,
            "set_pause_timer": self._set_pause_timer,
            "set_verbose_log": self._set_verbose_log,
            "set_tasks": self._set_tasks,
            "set_save_json_db": self._set_save_json_db,
            "set_save_json_file": self._set_save_json_file,
            "import_geos_from_csv": self._import_geos_from_csv
        }

        await websocket.accept()
        self.user_connections.append(websocket)
        try:
            self.bot_manager.get_set_process(target=websocket)
            self._get_num_tasks(target_ws=websocket)
            await self._get_num_workers(target_ws=websocket)
            await self._get_scheduler_interval(target_ws=websocket)
            await self._get_pause_timer(target_ws=websocket)
            await self._get_verbose_log(target_ws=websocket)
            await self._get_save_json_db(target_ws=websocket)
            await self._get_save_json_file(target_ws=websocket)
            while True:
                message = await websocket.receive_json()
                handler = action_map.get(message.get("action"))
                print(message)
                if handler:
                    data = message.get("data")
                    if inspect.iscoroutinefunction(handler):
                        await handler(data=data)
                    else:
                        handler(data=data)
                else:
                    self.logger(f"Unknown action : {message.get('action')}", force=True)
                    print("Action Error!")
        except WebSocketDisconnect:
            try:
                self.user_connections.remove(websocket)
                print("WebSocket connection removed")
            except ValueError:
                print("WebSocket connection not found in list")
        except Exception as e:
            print(f"WebSocket error: {e}")
            try:
                self.user_connections.remove(websocket)
            except ValueError:
                pass

    async def _set_pause_timer(self, data=None):
        self.pause_timer = not self.pause_timer
        status = "paused" if self.pause_timer else "resumed"
        await self._get_pause_timer()
        self.logger(f"Timer has been {status}.", force=True)
    
    async def _set_verbose_log(self, data=None):
        self.verbose_log = not self.verbose_log
        status = "enabled" if self.verbose_log else "disabled"
        await self._get_verbose_log()
        self.logger(f"Verbose logging has been {status}.", force=True)

    async def _set_scheduler_interval(self, data):
        self.scheduler_interval = data
        self._reset_timer()
        await self._get_scheduler_interval()
        self.logger(f"Scheduler interval set to {self.scheduler_interval+1}.", force=True)

    async def _add_worker(self, data=None):
        self.bot_manager.add_worker()
        await self._get_num_workers()

    async def _remove_worker(self, data=None):
        self.bot_manager.remove_worker()
        await self._get_num_workers()

    async def _set_process(self, data=False):
        if not self.bot_manager.in_process:
            self.run_scheduler(new_session=data)
            #threading.Thread(target=self.bot_manager.run_workers).start()
        else:
            self.bot_manager.stop_workers()

    def _set_tasks(self, data=None):
        try:
            self.bot_manager.task_manager_init()
        except Exception as e:
            self.logger(f"Error: {e}", force=True)

    async def _set_save_json_db(self, data=None):
        self.bot_manager.save_json_db = not self.bot_manager.save_json_db
        await self._get_save_json_db()
        message = "JSON-DB saving enabled" if self.bot_manager.save_json_db else "JSON-DB saving disabled"
        self.logger(message, force=True)

    async def _set_save_json_file(self, data=None):
        self.bot_manager.save_json_file = not self.bot_manager.save_json_file
        await self._get_save_json_file()
        message = f"JSON file saving enabled | Dir Path: {config.JSON_LOG_DIR}" if self.bot_manager.save_json_file else "JSON file saving disabled"
        self.logger(message, force=True)

    def _import_geos_from_csv(self, data=None):
        def import_geos():
            threads = []
            for country in config.COUNTRY_CONFIG:
                t = threading.Thread(
                    target=self.bot_manager.import_geo,
                    kwargs=dict(
                        csv_path=country["csv"],
                        sep=country["sep"],
                        country_name=country["name"],
                        country_vat=country["vat"],
                        country_currency=country["currency"],
                        province_header=country["province"],
                        city_header=country["city"],
                        additional_header=country["additional"],
                        postal_code_header=country["postal"]
                    ),
                    daemon=True
                )
                t.start()
                threads.append(t)
            
            for t in threads:
                t.join()
            
            self.bot_manager.task_manager_init()
            self.logger("Import completed!", force=True)
        
        threading.Thread(target=import_geos, daemon=True).start()

    #################################################

    async def _get_num_workers(self, target_ws=None):
        message = {"action": "get_num_workers", "data": len(self.bot_manager.worker_manager_list)}
        await self._send_message(message, target_ws=target_ws)

    async def _get_scheduler_interval(self, target_ws=None):
        message = {"action": "get_scheduler_interval", "data": self.scheduler_interval}
        await self._send_message(message, target_ws=target_ws)

    async def _get_pause_timer(self, target_ws=None):
        message = {"action": "get_pause_timer", "data": self.pause_timer}
        await self._send_message(message, target_ws=target_ws)

    async def _get_verbose_log(self, target_ws=None):
        message = {"action": "get_verbose_log", "data": self.verbose_log}
        await self._send_message(message, target_ws=target_ws)
    
    async def _get_save_json_db(self, target_ws=None):
        message = {"action": "get_save_json_db", "data": self.bot_manager.save_json_db}
        await self._send_message(message, target_ws=target_ws)
    
    async def _get_save_json_file(self, target_ws=None):
        message = {"action": "get_save_json_file", "data": self.bot_manager.save_json_file}
        await self._send_message(message, target_ws=target_ws)
    
    def _get_num_tasks(self, target_ws=None):
        for task_manager in self.bot_manager.task_manager_list:
            task_manager.info(target=target_ws)