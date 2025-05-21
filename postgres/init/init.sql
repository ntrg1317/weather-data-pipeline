
--============================================================--
--                      Extensions                            --
--============================================================--
-- required: postgis
-- optional: timescaledb

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;
-- CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;
       
-- DROP SCHEMA IF EXISTS isd CASCADE;
CREATE SCHEMA IF NOT EXISTS isd;


--============================================================--
--                        Tables                              --
--============================================================--

------------------------------------------------
-- isd.station : station meta data
-- isd.history : station observation history
-- isd.mwcode  : weather code dict
-- isd.element : weather element dict
-- isd.world   : world country metadata
-- isd.china   : china adcode metadata
------------------------------------------------


------------------------------------------------
-- isd_station
--   Station meta data
------------------------------------------------
-- DROP TABLE IF EXISTS isd.station;
CREATE TABLE isd.station
(
    station    VARCHAR(12) PRIMARY KEY,
    usaf       VARCHAR(6) GENERATED ALWAYS AS (substring(station, 1, 6)) STORED,
    wban       VARCHAR(5) GENERATED ALWAYS AS (substring(station, 7, 5)) STORED,
    name       VARCHAR(32),
    country    VARCHAR(2),
    province   VARCHAR(2),
    icao       VARCHAR(4),
    location   GEOMETRY(POINT),
    longitude  NUMERIC GENERATED ALWAYS AS (Round(ST_X(location)::NUMERIC, 6)) STORED,
    latitude   NUMERIC GENERATED ALWAYS AS (Round(ST_Y(location)::NUMERIC, 6)) STORED,
    elevation  NUMERIC,
    period     daterange,
    begin_date DATE GENERATED ALWAYS AS (lower(period)) STORED,
    end_date   DATE GENERATED ALWAYS AS (upper(period)) STORED
);

COMMENT ON TABLE  isd.station IS 'Integrated Surface Data (isd) dataset station history';
COMMENT ON COLUMN isd.station.station IS 'Primary key of isd station, 11 char, usaf+wban';
COMMENT ON COLUMN isd.station.usaf IS 'Air Force station ID. May contain a letter in the first position.';
COMMENT ON COLUMN isd.station.wban IS 'NCDC WBAN number';
COMMENT ON COLUMN isd.station.name IS 'Station name';
COMMENT ON COLUMN isd.station.country IS 'FIPS country ID (2 char)';
COMMENT ON COLUMN isd.station.province IS 'State for US stations (2 char)';
COMMENT ON COLUMN isd.station.icao IS 'ICAO ID';
COMMENT ON COLUMN isd.station.location IS '2D location of station';
COMMENT ON COLUMN isd.station.longitude IS 'longitude of the station';
COMMENT ON COLUMN isd.station.latitude IS 'latitude of the station';
COMMENT ON COLUMN isd.station.elevation IS 'altitude of the station';
COMMENT ON COLUMN isd.station.begin_date IS 'Beginning Period Of Record (YYYYMMDD). There may be reporting gaps within the P.O.R.';
COMMENT ON COLUMN isd.station.end_date IS 'Ending Period Of Record (YYYYMMDD). There may be reporting gaps within the P.O.R.';
COMMENT ON COLUMN isd.station.period IS 'range of [begin,end] period';

-- indexes
CREATE INDEX IF NOT EXISTS station_usaf_idx ON isd.station (usaf);
CREATE INDEX IF NOT EXISTS station_wban_idx ON isd.station (wban);
CREATE INDEX IF NOT EXISTS station_name_idx ON isd.station (name);
CREATE INDEX IF NOT EXISTS station_icao_idx ON isd.station (icao);
CREATE INDEX IF NOT EXISTS station_begin_date_idx ON isd.station (begin_date);
CREATE INDEX IF NOT EXISTS station_end_date_idx ON isd.station (end_date);
CREATE INDEX IF NOT EXISTS station_location_idx ON isd.station USING GIST (location);
CREATE INDEX IF NOT EXISTS station_period_idx ON isd.station USING GIST (period);


------------------------------------------------
-- isd_history
--   Station historic observation summary
------------------------------------------------
-- DROP TABLE IF EXISTS isd.history;
CREATE TABLE IF NOT EXISTS isd.history
(
    station      VARCHAR(12),
    year         DATE,
    usaf         VARCHAR(6) GENERATED ALWAYS AS (substring(station, 1, 6)) STORED,
    wban         VARCHAR(5) GENERATED ALWAYS AS (substring(station, 7, 5)) STORED,
    country      VARCHAR(2), -- materialized 2 char country code
    active_month INTEGER,    -- how many month have more than 1 observation record ?
    total        INTEGER NOT NULL,
    m1           INTEGER,
    m2           INTEGER,
    m3           INTEGER,
    m4           INTEGER,
    m5           INTEGER,
    m6           INTEGER,
    m7           INTEGER,
    m8           INTEGER,
    m9           INTEGER,
    m10          INTEGER,
    m11          INTEGER,
    m12          INTEGER,
    PRIMARY KEY (station, year)
);

COMMENT ON TABLE  isd.history IS 'isd Observation History';
COMMENT ON COLUMN isd.history.station IS 'station name: usaf(6) + wban(5)';
COMMENT ON COLUMN isd.history.year IS 'observe year';
COMMENT ON COLUMN isd.history.usaf IS 'Air Force station ID(6). May contain a letter in the first position';
COMMENT ON COLUMN isd.history.wban IS 'NCDC WBAN number, 5char';
COMMENT ON COLUMN isd.history.country IS '2 Char Country Code, N/A by default';
COMMENT ON COLUMN isd.history.active_month IS 'How many months that has observation records in this year?';
COMMENT ON COLUMN isd.history.total IS 'How many observation in this year';
COMMENT ON COLUMN isd.history.m1 IS 'Jan records';
COMMENT ON COLUMN isd.history.m2 IS 'Feb records';
COMMENT ON COLUMN isd.history.m3 IS 'Mar records';
COMMENT ON COLUMN isd.history.m4 IS 'Apr records';
COMMENT ON COLUMN isd.history.m5 IS 'May records';
COMMENT ON COLUMN isd.history.m6 IS 'Jun records';
COMMENT ON COLUMN isd.history.m7 IS 'Jul records';
COMMENT ON COLUMN isd.history.m8 IS 'Aug records';
COMMENT ON COLUMN isd.history.m9 IS 'Sep records';
COMMENT ON COLUMN isd.history.m10 IS 'Oct records';
COMMENT ON COLUMN isd.history.m11 IS 'Sep records';
COMMENT ON COLUMN isd.history.m12 IS 'Dec records';

-- indexes
CREATE UNIQUE INDEX IF NOT EXISTS history_station_year_idx ON isd.history (station, year);
CREATE INDEX IF NOT EXISTS history_year_country_idx ON isd.history (year, country);
CREATE INDEX IF NOT EXISTS history_usaf_idx ON isd.history (usaf);


------------------------------------------------
-- isd_mwcode
--   Weather code used by isd MW fields
------------------------------------------------
-- DROP TABLE IF EXISTS isd.mwcode;
CREATE TABLE IF NOT EXISTS isd.mwcode
(
    code  VARCHAR(2) PRIMARY KEY,
    name  VARCHAR(8),
    brief TEXT
);

COMMENT ON TABLE  isd.mwcode IS 'isd MW Code Dictionary';
COMMENT ON COLUMN isd.mwcode.code IS '2 digit code from 00-99';
COMMENT ON COLUMN isd.mwcode.name IS 'short description in chinese';
COMMENT ON COLUMN isd.mwcode.brief IS 'origin description text';


------------------------------------------------
-- isd.element
--   Meteorology elements dictionary
------------------------------------------------
-- DROP TABLE IF EXISTS isd.element;
CREATE TABLE IF NOT EXISTS isd.element
(
    id       TEXT PRIMARY KEY,
    name     TEXT,
    name_cn  TEXT,
    coverage NUMERIC,
    section  TEXT
);

COMMENT ON TABLE  isd.element IS 'isd element dictionary';
COMMENT ON COLUMN isd.element.id IS 'element identifer';
COMMENT ON COLUMN isd.element.name IS 'element name';
COMMENT ON COLUMN isd.element.name_cn IS 'element cn name';
COMMENT ON COLUMN isd.element.coverage IS 'element coverage';
COMMENT ON COLUMN isd.element.section IS 'element category';


------------------------------------------------
-- isd.world
--   Countries & Regions of the world
------------------------------------------------
-- DROP TABLE IF EXISTS isd.world;
CREATE TABLE IF NOT EXISTS isd.world
(
    id            INTEGER    NOT NULL PRIMARY KEY,
    iso2          VARCHAR(3) NOT NULL UNIQUE,
    iso3          VARCHAR(3) NOT NULL UNIQUE,
    ison          VARCHAR(3) NOT NULL UNIQUE,
    name          VARCHAR(64) UNIQUE,
    name_local    varchar(256) UNIQUE,
    name_en       VARCHAR(64) UNIQUE,
    name_cn       VARCHAR(64) UNIQUE,
    name_fr       VARCHAR(64),
    name_ru       VARCHAR(64),
    name_es       VARCHAR(64),
    name_ar       VARCHAR(64),
    tld           VARCHAR(3),
    lang          VARCHAR(5)[],
    currency      VARCHAR(3)[],
    cur_name      VARCHAR(32)[],
    cur_code      VARCHAR(3)[],
    dial          VARCHAR(18),
    wmo           VARCHAR(2),
    itu           VARCHAR(3),
    ioc           VARCHAR(3) UNIQUE,
    fifa          VARCHAR(16) UNIQUE,
    gaul          VARCHAR(6) UNIQUE,
    edgar         VARCHAR(2) UNIQUE,
    marc          VARCHAR(16),
    fips          VARCHAR(32),
    m49           VARCHAR(3),
    m49_marco_id  VARCHAR(3),
    m49_marco     VARCHAR(8),
    m49_middle_id VARCHAR(3),
    m49_middle    VARCHAR(16),
    m49_sub_id    VARCHAR(3),
    m49_sub       VARCHAR(32),
    is_ldc        BOOLEAN,
    is_lldc       BOOLEAN,
    is_sids       BOOLEAN,
    is_developed  BOOLEAN,
    is_sovereign  BOOLEAN,
    remark        VARCHAR(32),
    continent     VARCHAR(2),
    capital       VARCHAR(32),
    center        Geometry(Point, 4326),
    fence         Geometry
);

CREATE INDEX IF NOT EXISTS world_iso2_idx ON isd.world (iso2);
CREATE INDEX IF NOT EXISTS world_iso3_idx ON isd.world (iso3);
CREATE INDEX IF NOT EXISTS world_name_en_idx ON isd.world (name_en);
CREATE INDEX IF NOT EXISTS world_name_cn_idx ON isd.world (name_cn);
CREATE INDEX IF NOT EXISTS world_iso2_idx ON isd.world (iso2);
CREATE INDEX IF NOT EXISTS world_center_idx ON isd.world USING GIST(center);
CREATE INDEX IF NOT EXISTS world_fence_idx ON isd.world USING GIST(fence);

COMMENT ON TABLE   isd.world               IS '世界行政区划代码表';
COMMENT ON COLUMN  isd.world.id            IS 'iso3166 数值国家代码，主键';
COMMENT ON COLUMN  isd.world.iso2          IS 'iso3166 2位字符国家代码，唯一';
COMMENT ON COLUMN  isd.world.iso3          IS 'iso3166 3位字符国家代码，唯一';
COMMENT ON COLUMN  isd.world.ison          IS 'iso3166 3位数字国家代码，唯一';
COMMENT ON COLUMN  isd.world.name          IS '标准英文地名，唯一';
COMMENT ON COLUMN  isd.world.name_local    IS '本地语言名称，唯一';
COMMENT ON COLUMN  isd.world.name_en       IS '英文名称，唯一';
COMMENT ON COLUMN  isd.world.name_cn       IS '中文名称，唯一';
COMMENT ON COLUMN  isd.world.name_fr       IS '法文名称';
COMMENT ON COLUMN  isd.world.name_ru       IS '俄文名称';
COMMENT ON COLUMN  isd.world.name_es       IS '西班牙语名称';
COMMENT ON COLUMN  isd.world.name_ar       IS '阿拉伯文名称';
COMMENT ON COLUMN  isd.world.tld           IS '顶级域名';
COMMENT ON COLUMN  isd.world.lang          IS '使用的语言';
COMMENT ON COLUMN  isd.world.currency      IS '三位货币代码，多种为数组';
COMMENT ON COLUMN  isd.world.cur_name      IS '货币英文全称，多种为数组';
COMMENT ON COLUMN  isd.world.cur_code      IS '货币数值代码，多种为数组';
COMMENT ON COLUMN  isd.world.dial          IS '国际拨号代码';
COMMENT ON COLUMN  isd.world.wmo           IS '世界气象组织代码/WMO';
COMMENT ON COLUMN  isd.world.itu           IS '国际电信联盟代码/ITU';
COMMENT ON COLUMN  isd.world.ioc           IS '国际奥委会代码/IOC';
COMMENT ON COLUMN  isd.world.fifa          IS '国际足联代码/FIFA';
COMMENT ON COLUMN  isd.world.gaul          IS '国际粮农组织代码/FAO';
COMMENT ON COLUMN  isd.world.edgar         IS '美国证监会代码/SEC';
COMMENT ON COLUMN  isd.world.marc          IS '美国国会图书馆代码';
COMMENT ON COLUMN  isd.world.fips          IS '联邦信息处理代码/FIPS';
COMMENT ON COLUMN  isd.world.m49           IS '联合国M49区划代码';
COMMENT ON COLUMN  isd.world.m49_marco_id  IS 'M49 宏观地理区域编码';
COMMENT ON COLUMN  isd.world.m49_marco     IS 'M49 宏观地理区域英文名';
COMMENT ON COLUMN  isd.world.m49_middle_id IS 'M49 中间地理区域编码';
COMMENT ON COLUMN  isd.world.m49_middle    IS 'M49 中间地理区域英文名';
COMMENT ON COLUMN  isd.world.m49_sub_id    IS 'M49 地理亚区编码';
COMMENT ON COLUMN  isd.world.m49_sub       IS 'M49 地理亚区英文名';
COMMENT ON COLUMN  isd.world.is_ldc        IS 'LDC，是否为最不发达作家';
COMMENT ON COLUMN  isd.world.is_lldc       IS 'LLDC，是否为内陆发展中国家';
COMMENT ON COLUMN  isd.world.is_sids       IS 'SIDS，是否为小岛屿发展中国家';
COMMENT ON COLUMN  isd.world.is_developed  IS '是否为发达国家';
COMMENT ON COLUMN  isd.world.is_sovereign  IS '是否为主权国家';
COMMENT ON COLUMN  isd.world.remark        IS '备注：主权状态';
COMMENT ON COLUMN  isd.world.continent     IS '二位大洲名称';
COMMENT ON COLUMN  isd.world.capital       IS '首都英文名';
COMMENT ON COLUMN  isd.world.center        IS '地理中心位置点';
COMMENT ON COLUMN  isd.world.fence         IS '1:6千万地理围栏（均不包含争议地区）';


------------------------------------------------
-- isd.daily
--   Countries & Regions of the world
------------------------------------------------
-- DROP TABLE IF EXISTS isd.daily;
CREATE TABLE isd.daily(
    station_id VARCHAR(14) CONSTRAINT check_station_id_length CHECK (LENGTH(station_id) > 11 AND LENGTH(station_id) < 15),
    date date not null,
	n_records integer,
	air_temperature_avg decimal (15, 5),
	air_temperature_min decimal (15, 5),
	air_temperature_max decimal (15, 5),
	dew_point_avg decimal (15, 5),
	dew_point_min decimal (15, 5),
	dew_point_max decimal (15, 5),
	sea_lvl_pressure_avg decimal (15, 5),
	sea_lvl_pressure_min decimal (15, 5),
	sea_lvl_pressure_max decimal (15, 5),
	wind_direction decimal (15, 5),
	wind_speed_avg decimal (15, 5),
	wind_speed_min decimal (15, 5),
	wind_speed_max decimal (15, 5),
	sky_condition integer,
	one_hour_precipitation_avg decimal (15, 5),
	one_hour_precipitation_min decimal (15, 5),
	one_hour_precipitation_max decimal (15, 5),
	six_hour_precipitation_avg decimal (15, 5),
	six_hour_precipitation_min decimal (15, 5),
	six_hour_precipitation_max decimal (15, 5),
    primary key(station_id, date)
);



--============================================================--
--                   Function Definition                       -
--============================================================--

------------------------------------------------
-- wind16
--   turn 360 degree angle to 16 compass direction
------------------------------------------------
CREATE OR REPLACE FUNCTION isd.wind16(_i NUMERIC) RETURNS VARCHAR(3) AS
$$
SELECT CASE width_bucket(_i, 0, 360, 16) - 1
           WHEN 0 THEN 'N' WHEN 1 THEN 'NNE' WHEN 2 THEN 'NE' WHEN 3 THEN 'ENE'
           WHEN 4 THEN 'E' WHEN 5 THEN 'ESE' WHEN 6 THEN 'SE' WHEN 7 THEN 'SSE'
           WHEN 8 THEN 'S' WHEN 9 THEN 'SSW' WHEN 10 THEN 'SW' WHEN 11 THEN 'WSW'
           WHEN 12 THEN 'W' WHEN 13 THEN 'WNW' WHEN 14 THEN 'NW' WHEN 15 THEN 'NNW'
           WHEN NULL THEN 'C'
           END;
$$ LANGUAGE SQL IMMUTABLE;
COMMENT ON FUNCTION isd.wind16(_i NUMERIC) IS 'turn 0-360 degree to 16-compass marks';

------------------------------------------------
-- wind8
--   turn 360 degree angle to 8 compass direction
------------------------------------------------
CREATE OR REPLACE FUNCTION isd.wind8(_i NUMERIC) RETURNS VARCHAR(3) AS
$$
SELECT CASE width_bucket(_i, 0, 360, 8) - 1
           WHEN 0 THEN 'N' WHEN 1 THEN 'NE' WHEN 2 THEN 'E'
           WHEN 3 THEN 'SE' WHEN 4 THEN 'S' WHEN 5 THEN 'SW'
           WHEN 6 THEN 'W' WHEN 7 THEN 'NW' WHEN NULL THEN 'C'
           END;
$$ LANGUAGE SQL IMMUTABLE;
COMMENT ON FUNCTION isd.wind8(_i NUMERIC) IS 'turn 0-360 degree to 8-compass marks';

------------------------------------------------
-- mwcode_name(mw_code text)
-- turn MW code into text representation
------------------------------------------------
-- CREATE OR REPLACE FUNCTION isd.mwcode_name(mw_code text) RETURNS TEXT
-- AS $$
-- SELECT CASE mw_code::INTEGER
--            WHEN 0 THEN '云不可测' WHEN 1 THEN '云渐消散' WHEN 2 THEN '天像不变' WHEN 3 THEN '云渐成型' WHEN 4 THEN '烟遮视线' WHEN 5 THEN '雾霭蒙蒙' WHEN 6 THEN '灰尘弥漫' WHEN 7 THEN '风带灰尘' WHEN 8 THEN '风卷尘漫' WHEN 9 THEN '沙尘暴'
--            WHEN 10 THEN '薄雾弥漫' WHEN 11 THEN '薄雾零散' WHEN 12 THEN '薄雾成片' WHEN 13 THEN '可见闪电' WHEN 14 THEN '雨不落地' WHEN 15 THEN '雨落地面' WHEN 16 THEN '雨落测站' WHEN 17 THEN '雷鸣电闪' WHEN 18 THEN '雨飑风啸' WHEN 19 THEN '漏斗龙卷'
--            WHEN 20 THEN '毛毛细雨' WHEN 21 THEN '细雨不落' WHEN 22 THEN '细雪不落' WHEN 23 THEN '细雨冰霜' WHEN 24 THEN '毛毛冰雨' WHEN 25 THEN '阵雨' WHEN 26 THEN '阵雪' WHEN 27 THEN '阵冰雹' WHEN 28 THEN '雾与冰雾' WHEN 29 THEN '雷暴'
--            WHEN 30 THEN '轻沙暴渐缓' WHEN 31 THEN '轻沙暴维持' WHEN 32 THEN '轻沙暴增强' WHEN 33 THEN '强沙暴渐缓' WHEN 34 THEN '强沙暴维持' WHEN 35 THEN '强沙暴增强' WHEN 36 THEN '轻飘雪减缓' WHEN 37 THEN '重飘雪减缓' WHEN 38 THEN '吹雪渐强' WHEN 39 THEN '重飘雪走强'
--            WHEN 40 THEN '远方有雾' WHEN 41 THEN '片状雾' WHEN 42 THEN '雾缓天见' WHEN 43 THEN '雾缓天蔽' WHEN 44 THEN '雾恒天见' WHEN 45 THEN '雾恒天蔽' WHEN 46 THEN '雾浓天见' WHEN 47 THEN '雾浓天蔽' WHEN 48 THEN '雾凇天见' WHEN 49 THEN '雾凇天蔽'
--            WHEN 50 THEN '雷断续毛毛雨' WHEN 51 THEN '雷持续毛毛雨' WHEN 52 THEN '雷中毛毛雨' WHEN 53 THEN '雷中毛雨' WHEN 54 THEN '雷大毛毛雨' WHEN 55 THEN '雷大毛毛雨' WHEN 56 THEN '雷冻小毛毛雨' WHEN 57 THEN '雷冻大毛毛雨' WHEN 58 THEN '雷轻毛毛雨' WHEN 59 THEN '雷重毛毛雨'
--            WHEN 60 THEN '雷雨断续轻' WHEN 61 THEN '雷雨持续轻' WHEN 62 THEN '雷雨断续中' WHEN 63 THEN '雷雨持续中' WHEN 64 THEN '雷雨断续大' WHEN 65 THEN '雷雨持续大' WHEN 66 THEN '雷冻雨轻' WHEN 67 THEN '雷冻雨重' WHEN 68 THEN '雷冻雪轻' WHEN 69 THEN '雷冻雪重'
--            WHEN 70 THEN '雷雪断续轻' WHEN 71 THEN '雷雪持续轻' WHEN 72 THEN '雷雪断续中' WHEN 73 THEN '雷雪持续中' WHEN 74 THEN '雷雪断续重' WHEN 75 THEN '雷雪持续中' WHEN 76 THEN '钻石星尘' WHEN 77 THEN '雪粒' WHEN 78 THEN '独立大雪花' WHEN 79 THEN '冰颗粒'
--            WHEN 80 THEN '雷雨轻' WHEN 81 THEN '雷雨重' WHEN 82 THEN '雷夹特大雨' WHEN 83 THEN '雷雨夹雪轻' WHEN 84 THEN '雷雨夹雪重' WHEN 85 THEN '雷雪轻' WHEN 86 THEN '雷雪重' WHEN 87 THEN '小冰雹' WHEN 88 THEN '大冰雹' WHEN 89 THEN '无雷冰雹轻'
--            WHEN 90 THEN '无雷冰雹重' WHEN 91 THEN '轻雨带阵雷' WHEN 92 THEN '重雨带阵雷' WHEN 93 THEN '轻雪带阵雷' WHEN 94 THEN '重雪带阵雷' WHEN 95 THEN '雷暴雨无冰雹' WHEN 96 THEN '雷暴夹冰雹' WHEN 97 THEN '雷暴带雨无雹' WHEN 98 THEN '雷暴夹沙暴' WHEN 99 THEN '重雷暴夹冰雹'
--            ELSE '' END;
-- $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
-- COMMENT ON FUNCTION isd.mwcode_name(mw_code text) IS 'turn 2-char MW weather code into human readable string';