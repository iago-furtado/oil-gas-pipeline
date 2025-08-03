-- Query 1: Total oil and gas production for West Virginia in the last 12 months
SELECT
    SUM(oil_volume) AS total_oil_volume,
    SUM(gas_volume) AS total_gas_volume
FROM
    production
WHERE
    state = 'West Virginia'
    AND date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH);


-- Query 2: Which county has the highest number of wells?
SELECT
    county,
    COUNT(*) AS total_wells
FROM
    wells
GROUP BY
    county
ORDER BY
    total_wells DESC
LIMIT 1;


-- Query 3 – Simulated Join with Explanation
-- Since the `production` table does not contain a `well_id`,
-- we're simulating a general join by dividing total production
-- by the total number of wells in the same state.
SELECT
    ROUND(SUM(p.oil_volume) / COUNT(w.id), 2) AS avg_oil_per_well,
    ROUND(SUM(p.gas_volume) / COUNT(w.id), 2) AS avg_gas_per_well
FROM
    production p
JOIN
    wells w
ON
    p.state = w.status  -- Simulated connection: production state ≈ well status
WHERE
    p.state = 'West Virginia';


-- Query 4 – YoY Production Variation
WITH yearly_production AS (
    SELECT
        STRFTIME('%Y', date) AS year,
        SUM(oil_volume) AS total_oil,
        SUM(gas_volume) AS total_gas
    FROM production
    GROUP BY year
    ),
    yoy_comparison AS (
        SELECT
            curr.year,
            curr.total_oil,
            curr.total_gas,
            prev.total_oil AS previous_oil,
            prev.total_gas AS previous_gas,
            ROUND( ((curr.total_oil - prev.total_oil)*1.0 / prev.total_oil) * 100, 2) AS oil_yoy_change_percent,
            ROUND( ((curr.total_gas - prev.total_gas)*1.0 / prev.total_gas) * 100, 2) AS gas_yoy_change_percent
        FROM yearly_production curr
        LEFT JOIN yearly_production prev
        ON CAST(curr.year AS INTEGER) = CAST(prev.year AS INTEGER) + 1
    )
    SELECT * FROM yoy_comparison
    ORDER BY year;