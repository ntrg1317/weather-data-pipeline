import concurrent.futures
from dataclasses import dataclass, field
from threading import Thread
from typing import Dict, List, Optional, Union
import csv
import gzip
import json
import logging
import os
import tarfile
import time



def parse_metric(metric, missing_value: str, scale: int):
    if metric == missing_value:
        return ""
    try:
        v = int(metric)
        if scale == 1:
            return str(v)
        elif scale == 10:
            return f"{v / 10:.1f}"
        elif scale == 100:
            return f"{v / 100:.2f}"
        elif scale == 1000:
            return f"{v / 1000:.3f}"
        return ""
    except ValueError:
        return ""

@dataclass
class Record:
    StationId: str = ""
    Timestamp: str = "" #UTC

    #Wind
    WindAngle: str = ""
    WindSpeed: str = ""
    WindTypeCode: str = ""

    #Cloud
    CloudHeight: str = ""
    CloudMethodCode: str = ""
    CloudAVOK: str = "" # N Y 9 'Ceiling and Visibility Okay' condition

    #Visibility
    VisDistance: str = ""
    VisVariable: str = ""

    Temperature: str = ""
    DewPoint: str = ""
    Pressure: str = ""

    #Quality field
    WindAngleQC: str = ""
    WindSpeedQC: str = ""
    CloudHeightQC: str = ""
    VisDistanceQC: str = ""
    VisVariableQC: str = ""
    TemperatureQC: str = ""
    DewPointQC: str = ""
    PressureQC: str = ""

    CloudCode: str = ""
    StationPressure: str = ""
    WeatherManual: str = ""
    WeatherPresent: str = ""
    WeatherPast: str = ""
    WeatherPastHour: str = ""
    Precipitation: str = ""
    PrecipitationHour: str = ""
    PrecipitationCode: str = ""
    Gust: str = ""
    SnowDepth: str = ""

    Remarks: str = "" #remark data
    EQD: str = "" #element quality
    Data: str = "" #json data additional fields

    def _quality_codes(self):
        return "".join([
            self.WindAngleQC,
            self.WindSpeedQC,
            self.CloudHeightQC,
            self.CloudHeightQC,
            self.VisDistanceQC,
            self.VisVariableQC,
            self.TemperatureQC,
            self.DewPointQC,
            self.PressureQC
        ])

    def _format_record(self):
        return [
            self.StationId,
            self.Timestamp,
            self.Temperature,
            self.DewPoint,
            self.Pressure,
            self.StationPressure,
            self.VisDistance,
            self.WindAngle,
            self.WindSpeed,
            self.Gust,
            self.WindTypeCode,
            self.CloudHeight,
            self.CloudCode,
            self.SnowDepth,
            self.Precipitation,
            self.PrecipitationHour,
            self.PrecipitationCode,
            self.WeatherManual,
            self.WeatherPresent,
            self.WeatherPast,
            self.WeatherPastHour,
            self.Data,
        ]

@dataclass
class Station:
    ID: str = ""
    Year: str = ""
    Source: str = ""
    USAF: str = ""
    WBAN: str = ""
    Name: str = ""
    Longitude: str = ""
    Latitude: str = ""
    Elevation: str = ""
    ReportType: str = ""
    CallSign: str = ""
    QualityControl: str = ""
    AdditionalFields: List[str] = field(default_factory=list)
    Data: List[Record] = field(default_factory=list)

    def parse_station(self, data: List[List[str]], dedupe_mode: str) -> "Station":
        header = data[0]
        self.ID = header[0]
        self.Year = header[1][:4]
        self.Source = header[2]
        self.USAF = header[0][:6]
        self.WBAN = header[0][6:]
        self.Name = header[6]
        self.Latitude = float(header[3])
        self.Longitude = float(header[4])
        self.Elevation = float(header[5])
        self.ReportType = header[7]
        self.CallSign = header[8]
        self.QualityControl = header[9]
        if len(header) > 16:
            self.AdditionalFields = header[16:]

        if dedupe_mode == "tc-first":
            self.parse_data_ts_first(data[1:])

    def parse_data_ts_first(self, records: List[List[str]]):
        self.Data = []
        seen_ts = set()
        for record in records:
            ts = record[1]
            if ts not in seen_ts:
                seen_ts.add(ts)
                self.Data.append(self.parse_record(record))

    def parse_record(self, d: List[str]) -> Record:
        r = Record(StationId=d[0], Timestamp=d[1])

        wind = d[10].split(',')
        if len(wind) == 5:
            r.WindAngle = parse_metric(wind[0], "999", 1)
            r.WindAngleQC = wind[1][0]
            r.WindTypeCode = "C" if wind[3] == "0000" and wind[2] == "9" else wind[2]
            r.WindSpeed = parse_metric(wind[3], "9999", 10)
            r.WindSpeedQC = wind[4][0]

        cloud = d[11].split(',')
        if len(cloud) == 4:
            r.CloudHeight = parse_metric(cloud[0], "99999", 1)
            r.CloudHeightQC = cloud[1][0]
            r.CloudMethodCode = cloud[2]
            r.CloudAVOK = cloud[3] if cloud[3] in ["N", "Y"] else ""

        vis = d[12].split(',')
        if len(vis) == 4:
            r.VisDistance = parse_metric(vis[0], "999999", 1)
            r.VisDistanceQC = vis[1][0]
            r.VisVariableQC = vis[3][0]
            r.VisVariable = vis[2] if vis[2] in ["N", "Y"] else ""

        temp = d[13].split(',')
        dew_point = d[14].split(',')
        sea_lvl_pressure = d[15].split(',')
        r.Temperature = parse_metric(temp[0], "+9999", 10)
        r.DewPoint = parse_metric(dew_point[0], "+9999", 10)
        r.Pressure = parse_metric(sea_lvl_pressure[0], "99999", 10)
        r.TemperatureQC = temp[1][0]
        r.PressureQC = dew_point[1][0]
        r.DewPointQC = dew_point[1][0]

        data = {
            "WND": d[10],
            "CIG": d[11],
            "VIS": d[12],
            "TMP": d[13],
            "DEW": d[14],
            "SLP": d[15],
        }

        for i in range(16, len(d)):
            if i - 16 < len(self.AdditionalFields):
                key = self.AdditionalFields[i - 16]
                value = d[i]
                if value == "":
                    continue

                if key == "GF1":
                    data[key] = value
                    cloud_code, cloud_code_qc = value[0:2], value[6]
                    if cloud_code == "99" or cloud_code_qc == "3" or cloud_code_qc == "7":
                        continue
                    r.CloudCode = value[0:2]
                elif key == "MA1":
                    data[key] = value
                    r.StationPressure = parse_metric(value[8:13], "99999", 10)
                elif key == "MW1":
                    data[key] = value
                    if value[3] == "3" or value[3] == "7":
                        continue
                    r.WeatherManual = value[0:2]
                elif key == "AW1":
                    data[key] = value
                    if value[3] == "3" or value[3] == "7":
                        continue
                    r.WeatherPresent = value[0:2]
                elif key == "AY1":
                    data[key] = value
                    if value[3] == "3" or value[3] == "7":
                        continue
                    r.WeatherPast = value[0:1]
                    r.WeatherPastHour = parse_metric(value[4:6], "99", 1)
                elif key in ["AA1", "AA2", "AA3", "AA4"] :
                    data[key] = value
                    if value[10] == "3" or value[3] == "7" or value[8] == "1" or value[8] == "9" or value[0:2] == "99":
                        continue

                    if r.Precipitation != "":
                        r.Precipitation = parse_metric(value[3:7], "9999", 10)
                        if r.Precipitation != "":
                            r.PrecipitationHour = parse_metric(value[0:2], "99", 1)
                            r.PrecipitationCode = value[8:9]
                elif key == "OC1":
                    data[key] = value
                    if value[5] == "3" or value[5] == "7":
                        continue
                    r.Gust = parse_metric(value[0:4], "9999", 10)
                elif key == "AJ1":
                    data[key] = value
                    if value[7] == "3" or value[7] == "7":
                        continue
                    r.SnowDepth = parse_metric(value[9:15], "999999", 10)
                else:
                    data[key] = value

            try:
                r.Data = json.dumps(data)
            except Exception as e:
                logging.error(f"JSON marshal error: {e}")

            return r


def parse_fixed_format_line(line):
    record = {
        'usaf_id': line[4:10],
        'wban_id': line[10:15],
        'date': line[15:23],  # YYYYMMDD
        'time': line[23:27],  # HHMM
        'latitude': float(line[28:34]) / 1000 if line[28:34].strip() != '+99999' else None,
        'longitude': float(line[34:41]) / 1000 if line[34:41].strip() != '+999999' else None,
        'report_type': line[41:46] if line[41:46].strip() != '99999' else None,
        'elevation': float(line[46:51]) / 10 if line[46:51].strip() else None,
        'wind_direction': int(line[60:63]) if line[60:63].strip() != '999' else None,
        'wind_speed': float(line[65:69]) / 10 if line[63:64].strip() != '9999' else None, # meters per second
        'sky_ceiling': int(line[70:75]) if line[70:75].strip() != '99999' else None,
        'visibility': int(line[78:84]) if line[78:84].strip() != '999999' else None, #meters
        'temp': float(line[87:92]) / 10 if line[87:92].strip() != '+9999' else None,
        'dew_point': float(line[93:98]) / 10 if line[93:98].strip() != '+9999' else None,
        'sea_lvl_pressure': float(line[99:104]) / 10 if line[99:104].strip() else None
    }

def parse_multiple():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(parse_fixed_format_line)