from services.bot.bot_manager import BotManager
from services.csv_manager import CSVManager
from services.table_manager import TableManager
from services.utils import config, confirm_action
from database.connection import Connection
from services.proxy_manager import ProxyManager
from typing import Dict

class DataManager:
    def __init__(self):
        self.db_connection: Connection = Connection()
        self.verbose_log = True
        self.bot_manager = BotManager(db_connection=self.db_connection, logger=self.logger)
        self.csv_manager = CSVManager(db_connection=self.db_connection, logger=self.logger)
        self.table_manager = TableManager(db_connection=self.db_connection, logger=self.logger)
        self.proxy_manager = ProxyManager(logger=self.logger)

    def logger(self, *args: object, force=False, action=None, target=None, raw=False) -> None:
        if self.verbose_log or force:
            args_str = " ".join(str(arg) for arg in args) if len(args) > 0 else ""
            if args_str:
                print(args_str)

    def run_bot(self, country: Dict[str, str]):
        self.bot_manager.add_task(country_config=country)
        self.bot_manager.add_worker()
        self.bot_manager.run_workers()
        
        self.bot_manager.remove_task(country_name=country["name"])
        self.bot_manager.remove_worker()
        print(f"Data for {country['name']} inserted.")

    def import_geo(self, country: Dict[str, str]):
        self.csv_manager.import_geo(
            csv_path=country["csv"],
            sep=country["sep"],
            country_name=country["name"],
            country_vat=country["vat"],
            country_currency=country["currency"],
            province_header=country["province"],
            city_header=country["city"],
            additional_header=country["additional"],
            postal_code_header=country["postal"]
        )
        print(f"Geographic data for {country['name']} inserted.")

    def menu(self):        
        menu_items = [
            ('Check Proxy IP', lambda: self.proxy_manager.check_ip()),
            ('Change Proxy IP', lambda: (
                self.proxy_manager.send_signal_newnym()
                if config.USE_PROXY else print("Change 'USE_PROXY=true' in config.json to use this service!")
            )),
            ('Import SQL File', lambda: self.table_manager.import_sql_file()),
            ('Create Tables', lambda: self.table_manager.create_tables()),
            ('Transform Bot JSON Data to Tabular', lambda: self.table_manager.tabular_transform()),
            ('Drop All Tables', lambda: (
                self.table_manager.drop_all_tables() if confirm_action("Drop all tables? (y/n): ") else print("Canceled.")
            )),
            ('Clear Bot Data Session', lambda: (
                self.table_manager.clear_bot_data_session() if confirm_action("Clear bot data session? (y/n): ") else print("Canceled.")
            ))
        ]

        start_auto_menu = len(menu_items)+1

        while True:
            print("\n================= MENU =================")
            for i, (label, _) in enumerate(menu_items):
                print(f"{i+1}. {label}")
            
            print("=" * 40)
            for i, country in enumerate(config.COUNTRY_CONFIG, start=start_auto_menu):
                print(f"{i}. Insert Geographic Data ({country['name']})")
            
            print("=" * 40)
            for i, country in enumerate(config.COUNTRY_CONFIG, start=len(config.COUNTRY_CONFIG) + start_auto_menu):
                print(f"{i}. Fetch and Save Data ({country['name']})")
            
            print("=" * 40)
            print(f"{start_auto_menu + 2 * len(config.COUNTRY_CONFIG)}. Exit")

            choice = input("Input: ").strip()
            print()

            if choice.isdigit() and int(choice) < start_auto_menu:
                index = int(choice) - 1
                menu_items[index][1]()
            elif choice in map(str, range(start_auto_menu, start_auto_menu + len(config.COUNTRY_CONFIG))):
                index = int(choice) - start_auto_menu
                self.import_geo(config.COUNTRY_CONFIG[index])
            elif choice in map(str, range(len(config.COUNTRY_CONFIG)+start_auto_menu, start_auto_menu + 2 * len(config.COUNTRY_CONFIG))):
                index = int(choice) - (len(config.COUNTRY_CONFIG)+start_auto_menu)
                self.run_bot(config.COUNTRY_CONFIG[index])
            elif choice == str(start_auto_menu + 2 * len(config.COUNTRY_CONFIG)):
                break
            else:
                print("Invalid input!")

if __name__ == "__main__":
    DataManager().menu()