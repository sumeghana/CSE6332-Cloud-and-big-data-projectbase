-- Load the data from the given input file specified by parameter 'T', defining the columns explicitly.
data = LOAD '$T' USING PigStorage(',') AS (vendor_id:chararray, pickup_datetime:chararray, dropoff_datetime:chararray, passenger_count:int, trip_distance:float, rate_code_id:chararray, store_and_fwd_flag:chararray, pickup_longitude:float, pickup_latitude:float, dropoff_longitude:float, dropoff_latitude:float, payment_type:chararray, fare_amount:float, extra:float, mta_tax:float, tip_amount:float, tolls_amount:float, improvement_surcharge:float, total_amount:float);

-- Filter out the header (if starts with a non-numeric character in 'vendor_id', commonly a header)
filtered_data = FILTER data BY vendor_id MATCHES '\\d.*';

-- Compute the rounded trip distances and filter distances less than 200 miles
rounded_trips = FOREACH filtered_data GENERATE (int)ROUND(trip_distance) AS rounded_trip_distance, total_amount;
valid_trips = FILTER rounded_trips BY rounded_trip_distance < 200;

-- Group the data by the rounded_trip_distance
grouped_data = GROUP valid_trips BY rounded_trip_distance;

-- Calculate the average total amount for each group
avg_results = FOREACH grouped_data GENERATE group AS rounded_trip_distance, AVG(valid_trips.total_amount) AS avg_total_amount;

-- Output the results
DUMP avg_results;
