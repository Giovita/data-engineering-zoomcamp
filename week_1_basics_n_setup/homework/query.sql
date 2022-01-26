`Q3: Count records`

select tpep_pickup_datetime::DATE AS pup_day, count(1)
FROM yellow_taxi_trips
GROUP BY tpep_pickup_datetime::DATE
ORDER BY pup_day asc

'Q4: max tip'

select tpep_pickup_datetime::DATE AS pup_day, count(1), MAX(tip_amount) as max_tip
FROM yellow_taxi_trips
GROUP BY tpep_pickup_datetime::DATE
ORDER BY max_tip desc

`Q5 Most Popular Destination`
SELECT COUNT(1) as trips, zd."Zone"
FROM yellow_taxi_trips ytt
JOIN zones zp
on zp."LocationID" = ytt."PULocationID"
JOIN 
zones zd
on zd."LocationID" = ytt."DOLocationID"
WHERE CAST(tpep_pickup_datetime AS DATE) = '2021-01-14'
AND zp."Zone" = 'Central Park' 
GROUP BY zd."Zone"
order by trips desc

`q5 Most expensive route`
SELECT AVG(ytt."total_amount") as fare, zd."Zone", zp."Zone"
FROM yellow_taxi_trips ytt
JOIN zones zp
on zp."LocationID" = ytt."PULocationID"
JOIN 
zones zd
on zd."LocationID" = ytt."DOLocationID"
GROUP BY zd."Zone", zp."Zone"
order by fare desc