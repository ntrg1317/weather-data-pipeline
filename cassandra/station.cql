CREATE TABLE IF NOT EXISTS weather.station (
    wsid TEXT,
    name TEXT,
    country TEXT,
    province TEXT,
    icao TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation DOUBLE,
    begin_date DATE,
    end_date DATE,
    PRIMARY KEY ( wsid, country )
) WITH CLUSTERING ORDER BY (country DESC);

CREATE TABLE IF NOT EXISTS weather.history (
    wsid TEXT,
    year INT,
    total INT,
    active_month INT,
    m1 INT,
    m2 INT,
    m3 INT,
    m4 INT,
    m5 INT,
    m6 INT,
    m7 INT,
    m8 INT,
    m9 INT,
    m10 INT,
    m11 INT,
    m12 INT,
    PRIMARY KEY ((wsid), year)
) WITH CLUSTERING ORDER BY (year DESC);