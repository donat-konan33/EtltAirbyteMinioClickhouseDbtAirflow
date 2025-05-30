CREATE TABLE IF NOT EXISTS raw_depcode (
    geo_point_2d String,        -- often encoded to WKT, WKB or GeoJSON
    geo_shape String,           -- often encoded to WKT, WKB or GeoJSON
    reg_name String,
    reg_code String,
    dep_name_upper String,
    dep_current_code String,
    dep_status String
) ENGINE = MergeTree()
ORDER BY dep_current_code;
