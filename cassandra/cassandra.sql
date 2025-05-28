CREATE TABLE IF NOT EXISTS "weather"."hourly" (
    wsid text,                  // Composite of Air Force Datsav3 station number and NCDC WBAN number
    year int,                   // Year collected
    month int,                  // Month collected
    day int,                    // Day collected
    hour int,                   // Hour collected
    temperature double,         // Air temperature (degrees Celsius)
    dewpoint double,            // Dew point temperature (degrees Celsius)
    pressure double,            // Sea level pressure (hectopascals)
    wind_direction int,         // Wind direction in degrees. 0-359
    wind_speed double,          // Wind speed (meters per second)
    sky_condition int,          // Total cloud cover (coded, see format documentation)
    one_hour_precip double,     // One-hour accumulated liquid precipitation (millimeters)
    six_hour_precip double,     // Six-hour accumulated liquid precipitation (millimeters)
    PRIMARY KEY ((wsid), year, month, day, hour)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);

CREATE TABLE "weather"."station" (
    station      TEXT PRIMARY KEY,
    usaf         TEXT,
    wban         TEXT,
    name         TEXT,
    country      TEXT,
    province     TEXT,
    icao         TEXT,
    latitude     DOUBLE,
    longitude    DOUBLE,
    elevation    DOUBLE,
    begin_date   DATE,
    end_date     DATE
);
