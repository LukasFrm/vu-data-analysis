# Data dictionary

The final modelling table is `data/processed/judu_traffic_weather_hourly.parquet`. It contains one row per JUDU measurement point and local-clock hour after documented repeated-hour aggregation.

| Field | Parquet type | Unit / interpretation | Origin |
|---|---|---|---|
| `record_id` | int64 | ArcGIS record identifier; minimum retained when repeated-hour rows are combined | JUDU |
| `measurementpoint` | int32 | Detector or road-segment identifier | JUDU |
| `vehicle_count` | float32 | Published vehicle count for the detector-hour; modelling target | JUDU `count` |
| `timestamp_local` | timestamp | Hour start in Europe/Vilnius local clock time | JUDU `datefrom` |
| `hourfrom` | int8 | Published interval start hour | JUDU |
| `hourto` | int8 | Published interval end hour | JUDU |
| `divisor` | int16 | Published source field, retained without reinterpretation | JUDU |
| `datefrom_ms` | int64 | Original ArcGIS timestamp representation, milliseconds | JUDU |
| `segment_name` | string | Latest available road-segment label | JUDU lookup layer |
| `source_rows_aggregated` | int8 | Number of raw JUDU rows represented by the detector-hour | Derived |
| `temperature_2m` | float64 | Degrees Celsius | Open-Meteo |
| `relative_humidity_2m` | float64 | Percent | Open-Meteo |
| `snow_depth` | float64 | Metres | Open-Meteo |
| `cloud_cover` | float64 | Percent | Open-Meteo |
| `wind_speed_10m` | float64 | Kilometres per hour | Open-Meteo |
| `precipitation` | float64 | Millimetres during the hour | Open-Meteo |
| `rain` | float64 | Millimetres during the hour | Open-Meteo |
| `snowfall` | float64 | Centimetres during the hour | Open-Meteo |
| `weather_code` | int16 | WMO weather interpretation code | Open-Meteo |
| `timestamp_utc` | timezone-aware timestamp | UTC timestamp retained for traceability | Open-Meteo |
| `weather_hours_aggregated` | int8 | Number of physical weather hours represented by the local-clock hour | Derived |
| `is_rain` | boolean | True when hourly rain is greater than zero | Derived |
| `is_snow` | boolean | True when snowfall or snow depth is greater than zero | Derived |
| `is_freezing` | boolean | True when temperature is at or below 0 degrees Celsius | Derived |
| `hour` | int8 | Local hour, 0-23 | Derived |
| `weekday` | int8 | Monday=0 through Sunday=6 | Derived |
| `month` | int8 | Calendar month, 1-12 | Derived |
| `time_slot` | int16 | Weekday-hour category, `weekday * 24 + hour` | Derived |
| `is_mapped_road_point` | boolean | True for a point with a road label that is not marked as button-related data | Quality flag |
| `count_plausible` | boolean | True when `vehicle_count <= 10,000` | Quality flag |
| `model_eligible` | boolean | Mapped road point, plausible count and matched weather | Quality flag |

## Sampling and target definition

The intended sampling frequency is hourly per measurement point. Sensors can enter, leave or miss intervals, so the table is not a perfectly balanced panel. The source calls the target field `count`; because no verified source definition establishes that it is a continuous rate, the project describes it conservatively as JUDU's published vehicle count for the detector-hour.

## Daylight-saving treatment

JUDU exposes local-clock fields without explicit UTC offsets. Three detector-hour keys contained two rows in the repeated autumn 2025 hour; their vehicle counts were averaged to match the public widget's `AVG(count)` behavior. Open-Meteo was requested as Unix time, converted to Europe/Vilnius, and collapsed at the repeated autumn hour using sums for precipitation, rain and snowfall, means for continuous variables, and the maximum weather code. The nonexistent spring hour remains absent.
