CREATE INDEX IF NOT EXISTS idx_clean_pickup_ts ON clean.clean_yellow_trips (pickup_ts);
CREATE INDEX IF NOT EXISTS idx_clean_pu_pickup ON clean.clean_yellow_trips (pu_location_id, pickup_ts);
CREATE INDEX IF NOT EXISTS idx_clean_do ON clean.clean_yellow_trips (do_location_id);
