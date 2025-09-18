from database.models import TPostalArea, TValue, TDate, THour, TComponent
import services.utils
import json
from sqlalchemy import text
import sqlparse
from sqlalchemy.exc import IntegrityError
from typing import Callable
from database.connection import Connection
from typing import Any, Dict, Set
import threading

class TableManager:
    def __init__(self, db_connection: Connection, logger: Callable[..., None]):
        self.db_connection: Connection = db_connection
        self.logger: Callable[..., None] = logger
        self.existing_dates: Set[str] = set()
        self.existing_hours: Set[str] = set()
        self.existing_components: Set[str] = set()
        self._lock: threading.Lock = threading.Lock()

    def create_tables(self) -> None:
        try:
            self.db_connection.create_tables()
            self.logger("All tables created successfully.")
        except Exception as e:
            self.logger(f"Failed to create tables: {e}")

    def import_sql_file(self) -> None:
        with self.db_connection.get_session() as session:
            while True:
                filepath = input("File Path (or type 'exit' to quit): ").strip().strip('"').strip("'")
                if filepath.lower() == 'exit':
                    self.logger("Exiting.")
                    break
                if not filepath:
                    continue

                try:
                    with open(filepath, 'r', encoding='utf-8-sig') as file:
                        sql_commands = file.read()
                except FileNotFoundError:
                    self.logger("File not found!")
                    continue

                statements = sqlparse.split(sql_commands)
                success_count = 0
                error_count = 0

                for stmt in statements:
                    stmt = stmt.strip()
                    if not stmt:
                        continue
                    try:
                        session.execute(text(stmt))
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        self.logger("=" * 40)
                        self.logger("Error executing statement:")
                        self.logger(stmt[:500])
                        self.logger(f"🔺 Error: {str(e)[:500]}")
                        self.logger("=" * 40)

                try:
                    session.commit()
                except Exception as e:
                    self.logger("Commit failed. Rolling back.")
                    session.rollback()
                    self.logger(str(e)[:500])
                    continue

                self.logger(f"\nDone. {success_count} statements executed successfully. {error_count} failed.\n")

    def drop_all_tables(self) -> None:
        with self.db_connection.get_session() as session:
            drop_fks = """
            DECLARE @sql NVARCHAR(MAX) = N'';

            SELECT @sql += 'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) 
                + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';' + CHAR(13)
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id;

            EXEC sp_executesql @sql;
            """

            drop_tables = """
            DECLARE @sql NVARCHAR(MAX) = N'';

            SELECT @sql += 'DROP TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ';' + CHAR(13)
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id;

            EXEC sp_executesql @sql;
            """

            try:
                session.execute(text(drop_fks))
                session.execute(text(drop_tables))
                session.commit()
                self.logger("All tables dropped successfully.")
            except Exception as e:
                session.rollback()
                self.logger(f"Failed to drop tables: {e}")

    def clear_bot_data_session(self) -> None:
        with self.db_connection.get_session() as session:
            self.logger("Cleaning up pa_data...")
            """ for obj in session.query(database.models.TPostalArea).all():
                obj.pa_data = None
                obj.pa_status_code = None """
            try:
                query = """
                    UPDATE t_postal_area
                    SET pa_status_code = NULL,
                        pa_data = NULL;
                """
                session.execute(text(query))
                session.commit()
                self.logger("pa_data cleanup complete.")
            except Exception as e:
                session.rollback()
                self.logger(f"Cleanup failed: {e}")
                raise

    def _tabular_transform_init(self) -> None:
        with self.db_connection.get_session() as session:
            """Load ID"""
            # Load dates
            existing_date_ids = session.query(TDate.d_id).all()
            self.existing_dates = {date_id[0] for date_id in existing_date_ids}
            
            # Load hours
            existing_hour_ids = session.query(THour.h_id).all()
            self.existing_hours = {hour_id[0] for hour_id in existing_hour_ids}
            
            # Load components
            existing_component_ids = session.query(TComponent.co_id).all()
            self.existing_components = {comp_id[0] for comp_id in existing_component_ids}
            
            self.logger(f"\nCache initialized: {len(self.existing_dates)} dates, {len(self.existing_hours)} hours, {len(self.existing_components)} components")

    def _tabular_transform_tr(self, pa_id: str, json_data: Dict[str, Any], log: bool = False) -> None:
        with self.db_connection.get_session() as session:
            try:
                if "energy" not in json_data:
                    raise ValueError(f"Invalid JSON structure for postal area {pa_id}")

                for date_component_config in services.utils.config.DATE_COMPONENTS_CONFIG:
                    try:
                        hours_json = json_data["energy"][date_component_config]
                        
                        for hour_json in hours_json:
                            date = hour_json["date"]
                            hour = hour_json["hour"]

                            # Generate unique IDs
                            date_id = services.utils.md5_hash(date)
                            hour_id = services.utils.md5_hash(str(hour))

                            # Check cache
                            if date_id not in self.existing_dates:
                                t_date = TDate(d_id=date_id, d_date=date)
                                session.merge(t_date)
                                with self._lock:
                                    self.existing_dates.add(date_id)
                                session.flush()

                            if hour_id not in self.existing_hours:
                                t_hour = THour(h_id=hour_id, h_hour=hour)
                                session.merge(t_hour)
                                with self._lock:
                                    self.existing_hours.add(hour_id)
                                session.flush()
                            
                            for price_component in hour_json["priceComponents"]:
                                for price_component_config in services.utils.config.PRICE_COMPONENTS_CONFIG:
                                    if price_component["type"] in price_component_config["alias"]:
                                        component_id = services.utils.md5_hash(price_component_config["name"])
                                        if component_id not in self.existing_components:
                                            t_component = TComponent(
                                                co_id=component_id, 
                                                co_name=price_component_config["name"]
                                            )
                                            session.merge(t_component)
                                            with self._lock:
                                                self.existing_components.add(component_id)
                                            session.flush()

                                        t_value = TValue(
                                            pa_id=pa_id,
                                            d_id=date_id,
                                            h_id=hour_id,
                                            co_id=component_id, 
                                            v_value=price_component["priceExcludingVat"]
                                        )
                                        session.add(t_value)

                        session.commit()
                        if log:
                            self.logger(pa_id, f"TRANSFORM {date_component_config} | success")
                    except IntegrityError as e:
                        session.rollback()
                        if log:
                            self.logger(pa_id, f"TRANSFORM {date_component_config} | Primary key violation")
                        continue
            except Exception as e:
                session.rollback()
                self.logger(pa_id, f"Error transforming data: {str(e)}")
                raise

    def tabular_transform(self) -> None:
        with self.db_connection.get_session() as session:
            self._tabular_transform_init()
            
            areas = (
                session.query(TPostalArea.pa_id)
                .filter(TPostalArea.pa_data.isnot(None))
                .all()
            )

            if areas:
                self.logger(f"Found {len(areas)} rows!")
            else:
                self.logger("Nothing can be done!")
                return

            input_data = 0
            for index, area in enumerate(areas, start=1):
                try:
                    t_postal_area = (
                        session.query(TPostalArea.pa_data)
                        .filter(TPostalArea.pa_id == area.pa_id)
                        .first()
                    )

                    if not t_postal_area:
                        print(f"Postal area {area.pa_id} not found")
                        continue
                    
                    postal_json_data = json.loads(t_postal_area.pa_data)
                    
                    self._tabular_transform_tr(area.pa_id, postal_json_data)

                    input_data += 1
                except IntegrityError as e:
                    pass
                except Exception as e:
                    self.logger(f"\nError processing postal area {area.pa_id}: {e}")
                    continue

                if index % 10 == 0 or index == len(areas):
                    self.logger(f"\r{index}/{len(areas)} | New Tabular Data: {input_data}")

            self.logger("\nData transformation completed!")