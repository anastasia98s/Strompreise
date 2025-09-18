from sqlalchemy import Column, String, Text, ForeignKey, Date, Integer, Numeric, DECIMAL, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

model_base = declarative_base()

class TCountry(model_base):
    __tablename__ = 't_country'

    c_id = Column(String(32), primary_key=True)
    c_name = Column(String(255), nullable=False)
    c_vat = Column(DECIMAL(5, 2), default=0)
    c_currency = Column(DECIMAL(10, 8), default=1)

    provinces = relationship("TProvince", back_populates="country")


class TProvince(model_base):
    __tablename__ = 't_province'

    p_id = Column(String(32), primary_key=True)
    p_name = Column(String(255), nullable=False)
    c_id = Column(String(32), ForeignKey('t_country.c_id'), nullable=False)

    country = relationship("TCountry", back_populates="provinces")
    cities = relationship("TCity", back_populates="province")


class TCity(model_base):
    __tablename__ = 't_city'

    ci_id = Column(String(32), primary_key=True)
    ci_name = Column(String(255), nullable=False)
    p_id = Column(String(32), ForeignKey('t_province.p_id'), nullable=False)

    province = relationship("TProvince", back_populates="cities")
    postal_areas = relationship("TPostalArea", back_populates="city")


class TPostalArea(model_base):
    __tablename__ = 't_postal_area'

    pa_id = Column(String(32), primary_key=True)
    pa_name = Column(String(255))
    pa_code = Column(String(50), nullable=False)
    pa_data = Column(Text)  # JSON or any structured text
    pa_status_code = Column(Integer)
    ci_id = Column(String(32), ForeignKey('t_city.ci_id'), nullable=False)

    city = relationship("TCity", back_populates="postal_areas")
    values = relationship("TValue", back_populates="postal_area")


class TDate(model_base):
    __tablename__ = 't_date'

    d_id = Column(String(32), primary_key=True)
    d_date = Column(Date, nullable=False, unique=True)

    values = relationship("TValue", back_populates="date")


class THour(model_base):
    __tablename__ = 't_hour'

    h_id = Column(String(32), primary_key=True)
    h_hour = Column(Integer, nullable=False)

    values = relationship("TValue", back_populates="hour")


class TComponent(model_base):
    __tablename__ = 't_component'

    co_id = Column(String(32), primary_key=True)
    co_name = Column(String(255), nullable=False, unique=True)

    values = relationship("TValue", back_populates="component")


class TValue(model_base):
    __tablename__ = 't_value'

    pa_id = Column(String(32), ForeignKey('t_postal_area.pa_id'), primary_key=True)
    d_id = Column(String(32), ForeignKey('t_date.d_id'), primary_key=True)
    h_id = Column(String(32), ForeignKey('t_hour.h_id'), primary_key=True)
    co_id = Column(String(32), ForeignKey('t_component.co_id'), primary_key=True)
    v_value = Column(DECIMAL(10, 8))

    __table_args__ = (
        PrimaryKeyConstraint('pa_id', 'd_id', 'h_id', 'co_id', name='pk_t_value'),
    )

    postal_area = relationship("TPostalArea", back_populates="values")
    date = relationship("TDate", back_populates="values")
    hour = relationship("THour", back_populates="values")
    component = relationship("TComponent", back_populates="values")