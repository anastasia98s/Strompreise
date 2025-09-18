CREATE TABLE t_country (
    c_id VARCHAR(32) PRIMARY KEY,
    c_name VARCHAR(255) NOT NULL,
    c_vat DECIMAL(5,4) DEFAULT 0,
    c_currency DECIMAL(10,8) DEFAULT 1
);

CREATE TABLE t_province (
    p_id VARCHAR(32) PRIMARY KEY,
    p_name VARCHAR(255) NOT NULL,
    c_id VARCHAR(32) NOT NULL,
    FOREIGN KEY (c_id) REFERENCES t_country(c_id)
);

CREATE TABLE t_city (
    ci_id VARCHAR(32) PRIMARY KEY,
    ci_name VARCHAR(255) NOT NULL,
    p_id VARCHAR(32) NOT NULL,
    FOREIGN KEY (p_id) REFERENCES t_province(p_id)
);

CREATE TABLE t_postal_area (
    pa_id VARCHAR(32) PRIMARY KEY,
    pa_name VARCHAR(255),
    pa_code VARCHAR(50) NOT NULL,
    pa_data NVARCHAR(MAX),
    pa_status_code INTEGER,
    ci_id VARCHAR(32) NOT NULL,
    FOREIGN KEY (ci_id) REFERENCES t_city(ci_id)
);

CREATE TABLE t_date (
    d_id VARCHAR(32) PRIMARY KEY,
    d_date DATE NOT NULL UNIQUE
);

CREATE TABLE t_hour (
    h_id VARCHAR(32) PRIMARY KEY,
    h_hour INTEGER NOT NULL
);

CREATE TABLE t_component (
    co_id VARCHAR(32) PRIMARY KEY,
    co_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE t_value (
    pa_id VARCHAR(32),
    d_id VARCHAR(32),
    h_id VARCHAR(32),
    co_id VARCHAR(32),
    v_value DECIMAL(10,8),
    CONSTRAINT pk_t_price PRIMARY KEY (pa_id, d_id, h_id, co_id),
    FOREIGN KEY (pa_id) REFERENCES t_postal_area(pa_id),
    FOREIGN KEY (d_id) REFERENCES t_date(d_id),
    FOREIGN KEY (h_id) REFERENCES t_hour(h_id),
    FOREIGN KEY (co_id) REFERENCES t_component(co_id)
);