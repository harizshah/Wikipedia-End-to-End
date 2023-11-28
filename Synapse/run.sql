--top 5 stadiums by capacity
SELECT TOP 10 rank, stadium, capacity
FROM stadiums
ORDER BY capacity DESC

--Average capacity of stadium by region
SELECT region, AVG(capacity) avg_capacity
FROM stadiums
GROUP BY region
ORDER BY avg_capacity desc

--COUNT stadiums in each country
SELECT country, count(country) stadium_count
FROM stadiums
GROUP BY country
ORDER BY stadium_count DESC, country ASC

--stadium ranking with each region
SELECT rank, region, stadium,capacity,
        rank() OVER(PARTITION BY region ORDER BY capacity DESC) as region_rank
FROM stadiums

-- top 3 stadium ranking with each region
SELECT rank, region, stadium, capacity, region_rank FROM (
SELECT rank, region, stadium, capacity,
    RANK() OVER(PARTITION BY region ORDER BY capacity DESC) as region_rank
FROM stadiums ) as ranked_stadiums
WHERE region_rank <= 3

--stadiums with capacity above the average
SELECT stadium, t2.region, capacity, avg_capacity
FROM stadiums,
(SELECT region, AVG(capacity) avg_capacity FROM stadiums GROUP BY region) t2
WHERE t2.region = stadiums.region
and capacity > avg_capacity;

--stadium with closes capacity to regional median
WITH MedianCTE AS(
    SELECT region, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY capacity) OVER (PARTITION BY region) as median_capacity
    FROM stadiums
)

SELECT rank,stadium, region, capacity, ranked_stadiums.median_capacity, ranked_stadiums.median_rank
FROM(
    SELECT s.rank, s.stadium, s.region, s.capacity, m.median_capacity,
    ROW_NUMBER() OVER (PARTITION BY s.region ORDER BY ABS(s.capacity - m.median_capacity)) AS median_rank
    from stadiums s JOIN MedianCTE m ON s.region = m.region
) ranked_stadiums
WHERE median_rank = 1;