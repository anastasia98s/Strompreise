import pandas as pd
from database.models import TCountry, TProvince, TCity, TPostalArea
import services.utils
from typing import Callable
from database.connection import Connection

class CSVManager:
    def __init__(self, db_connection: Connection, logger: Callable[..., None]):
        self.db_connection: Connection = db_connection
        self.logger: Callable[..., None] = logger

    def safe_lower(self, val) -> str:
        if pd.notna(val):
            return str(val).strip().lower()
        raise ValueError("Input value is NaN or None")

    def import_geo(self, csv_path, sep, country_name, country_vat, country_currency, province_header, city_header, additional_header, postal_code_header) -> None:
        try:
            with self.db_connection.get_session() as session:
                df = pd.read_csv(csv_path, sep=sep, dtype={postal_code_header: str})

                self.logger("0% [Country]", country_name, force=True)
                country_key = self.safe_lower(country_name)
                c_id = services.utils.md5_hash(country_key)
                country = session.query(TCountry).filter_by(c_id=c_id).first()
                if not country:
                    country = TCountry(c_id=c_id, c_name=country_name, c_vat=country_vat, c_currency=country_currency)
                    session.add(country)
                    session.commit()

                self.logger("25% [Province]", country_name, force=True)
                province_count = 0
                provinces_seen = set()
                for province_name in df[province_header].dropna().unique():
                    province_key = self.safe_lower(province_name)
                    p_id = services.utils.md5_hash(country_key + province_key)
                    if p_id not in provinces_seen:
                        provinces_seen.add(p_id)
                        if not session.query(TProvince).filter_by(p_id=p_id).first():
                            province = TProvince(p_id=p_id, p_name=province_name, c_id=c_id)
                            session.add(province)
                            province_count += 1
                session.commit()

                self.logger("50% [City]", country_name, force=True)
                city_count = 0
                cities_seen = set()
                for _, row in df.iterrows():
                    city_name = row.get(city_header)
                    province_name = row.get(province_header)
                    if pd.notna(city_name) and pd.notna(province_name):
                        city_key = self.safe_lower(city_name)
                        province_key = self.safe_lower(province_name)
                        ci_id = services.utils.md5_hash(country_key + province_key + city_key)
                        if ci_id not in cities_seen:
                            cities_seen.add(ci_id)
                            p_id = services.utils.md5_hash(country_key + province_key)
                            if not session.query(TCity).filter_by(ci_id=ci_id).first():
                                city = TCity(ci_id=ci_id, ci_name=city_name, p_id=p_id)
                                session.add(city)
                                city_count += 1
                session.commit()

                self.logger("75% [Postal Area]", country_name, force=True)
                postal_count = 0
                postal_seen = set()
                for _, row in df.iterrows():
                    city_name = row.get(city_header)
                    province_name = row.get(province_header)
                    additional = row.get(additional_header) if additional_header in row else None
                    postal_code = row.get(postal_code_header)

                    if pd.notna(city_name) and pd.notna(province_name) and pd.notna(postal_code):
                        postal_code = postal_code.replace(' ', '')
                        city_key = self.safe_lower(city_name)
                        province_key = self.safe_lower(province_name)
                        postal_key = self.safe_lower(postal_code)
                        
                        pa_id = services.utils.md5_hash(country_key + postal_key) # uniq postal pro country

                        if pa_id not in postal_seen:
                            postal_seen.add(pa_id)
                            pa_name = None
                            if pd.notna(additional) and str(additional).strip():
                                pa_name = str(additional).strip()

                            ci_id = services.utils.md5_hash(country_key + province_key + city_key)
                            if not session.query(TPostalArea).filter_by(pa_id=pa_id).first():
                                postal_area = TPostalArea(
                                    pa_id=pa_id,
                                    pa_name=pa_name,
                                    pa_code=postal_code,
                                    ci_id=ci_id
                                )
                                session.add(postal_area)
                                postal_count += 1
                session.commit()

                self.logger("100% [Done]", force=True)
                self.logger(f"\nImport Summary:")
                self.logger(f"➤  Country             : {country_name}", force=True)
                self.logger(f"➤  Provinces added     : {province_count}", force=True)
                self.logger(f"➤  Cities added        : {city_count}", force=True)
                self.logger(f"➤  Postal Areas added  : {postal_count}", force=True)
        except Exception as e:
            self.logger(f"Error: {e}", force=True)