CREATE KEYSPACE IF NOT EXISTS station WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };


CREATE TABLE IF NOT EXISTS station.station
(
    wsid       TEXT PRIMARY KEY,
    name       TEXT,
    country    TEXT,
    province   TEXT,
    icao       TEXT,
    latitude   DOUBLE,
    longitude  DOUBLE,
    elevation  DOUBLE,
    begin_date DATE,
    end_date   DATE
);

CREATE TABLE IF NOT EXISTS station.history (
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

CREATE TABLE IF NOT EXISTS station.country (
    fips TEXT,
    wsid TEXT,
    iso3 TEXT,
    iso2 TEXT,
    is_independent BOOLEAN,
    official_name_en TEXT,
    region TEXT,
    sub_region TEXT,
    capital TEXT,
    continent TEXT,
    currency TEXT,
    name TEXT,
    province TEXT,
    icao TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation DOUBLE,
    PRIMARY KEY ( (fips), wsid )
) WITH CLUSTERING ORDER BY (wsid ASC);

CREATE TABLE IF NOT EXISTS station.station_by_start_date(
    year INT,
    month INT,
    wsid TEXT,
    name TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation DOUBLE,
    begin_date DATE,
    PRIMARY KEY ( (year, month), wsid )
) WITH CLUSTERING ORDER BY (wsid ASC);

CREATE TABLE IF NOT EXISTS station.station_by_start_date(
    year INT,
    month INT,
    wsid TEXT,
    name TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation DOUBLE,
    begin_date DATE,
    PRIMARY KEY ( (year, month), wsid )
) WITH CLUSTERING ORDER BY (wsid ASC);