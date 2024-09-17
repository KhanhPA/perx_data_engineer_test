-- Tables definition: 

CREATE TABLE IF NOT EXISTS campaign (
    id INT PRIMARY KEY,
    name VARCHAR,
    status VARCHAR
);
CREATE TABLE reward_campaign (
    id INT PRIMARY KEY,
    Reward_name VARCHAR NOT NULL,
    updated_at timestamp NOT NULL,
    campaign_id INT,
    FOREIGN KEY (campaign_id) REFERENCES Campaign(id)
);

CREATE TABLE reward_transaction (
    id INT PRIMARY KEY,
    status VARCHAR NOT NULL,
    updated_at timestamp NOT NULL,
    Reward_campaigns_id INT,
    FOREIGN KEY (Reward_campaigns_id) REFERENCES Reward_campaigns(id)
);

CREATE TABLE campaign_reward_mapping (
    campaign_id INT,
    reward_id INT
);


CREATE TABLE IF NOT EXISTS http_log (
    "timestamp" timestamp,
    http_method VARCHAR,
    http_path VARCHAR,
    user_id VARCHAR
);


-- A) SQL test: 
-- Use case 1: 
    -- i will create a stored procedure in redshift to identify the most popular engagement day and time between a specific date range that can be input into the query.

    -- To identify the most popular engagement day and time between a specific date range for a campaign, i will need to join the campaign, 
    -- reward_campaign, and reward_transaction tables. I will convert the UTC updated_at timestamp to Singapore Time (SGT, UTC+8) and group the data by date and hour.

    -- The procedure will execute the query and store it results in a temp table, called temp_use_case1_output. 

CREATE OR REPLACE PROCEDURE use_case1(
    IN campaign_id INT,
    IN start_time TIMESTAMP,
    IN end_time TIMESTAMP
)
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS temp_use_case1_output (
        no_of_transactions INT,
        date DATE,
        time INT
    );

    DELETE FROM temp_use_case1_output;

    EXECUTE
    'INSERT INTO temp_use_case1_output
     SELECT 
         COUNT(rt.id) AS no_of_transactions,
         DATE(CONVERT_TIMEZONE(''UTC'', ''Asia/Singapore'', rt.updated_at)) AS date,
         DATEPART(hour, CONVERT_TIMEZONE(''UTC'', ''Asia/Singapore'', rt.updated_at)) AS time
     FROM 
         reward_transaction rt
     JOIN 
         reward_campaign rc ON rt.reward_campaigns_id = rc.id
     JOIN 
         campaign c ON rc.campaign_id = c.id
     WHERE 
         c.id = ' || campaign_id || '
         AND rt.updated_at BETWEEN ' || quote_literal(start_time) || ' 
         AND ' || quote_literal(end_time) || '
     GROUP BY 
         date, time
     ORDER BY 
         no_of_transactions DESC;';
END;
$$;
CALL use_case1(
    1,                             
    '2019-08-01 00:00:00',        
    '2019-08-31 23:59:59'          
);

SELECT * FROM temp_use_case1_output;


-- Use case 2:
    -- To compare the weekly performance of rewards, we'll use a CTE to calculate the redeemed count for the current and previous weeks and find the percentage difference.
    -- This query provides the weekly breakdown of rewards' redeemed counts and the percentage difference compared to the previous week.

    -- The LAG function is used to compare each week's count with the previous week's count, by using this window function we can get the 
    -- previous week's rewards count and then calculate the percentage difference between 2 weeks


WITH weekly_performance AS (
    SELECT
        rc.reward_name,
        DATE_TRUNC('week', rt.updated_at) AS week_start,
        COUNT(rt.id) AS reward_redeemed_count
    FROM
        reward_transaction rt
    JOIN
        reward_campaign rc ON rt.reward_campaigns_id = rc.id
    WHERE 
        rt.status = 'redeemed' 
    GROUP BY
        rc.reward_name,
        DATE_TRUNC('week', rt.updated_at)
),
performance_with_diff AS (
    SELECT
        wp.reward_name,
        wp.week_start,
        wp.reward_redeemed_count,
        LAG(wp.reward_redeemed_count) OVER (PARTITION BY wp.reward_name ORDER BY wp.week_start) AS previous_week_count
    FROM
        weekly_performance wp
)
    SELECT
        reward_name,
        reward_redeemed_count,
        ROUND(
            CASE 
                WHEN previous_week_count IS NULL THEN 0 
                WHEN previous_week_count = 0 THEN 100
                ELSE (reward_redeemed_count - previous_week_count)::NUMERIC/previous_week_count * 100
            END, 2
        ) AS percentage_difference,
        week_start
    FROM
        performance_with_diff
    ORDER BY
        reward_name,
        week_start;

-- B) Analyse HTTP Log for User Sessions: 

WITH parsed_http_log AS (
    -- Extract campaign and reward IDs from the http_path field
    SELECT
        user_id,
        "timestamp",
        http_method,
        http_path,
        CASE 
            WHEN http_method = 'GET' AND http_path LIKE '/campaigns/%' THEN split_part(http_path, '/', 3)
            ELSE NULL
        END AS campaign_id,
        CASE 
            WHEN http_method = 'POST' AND http_path LIKE '/rewards/%' THEN split_part(http_path, '/', 3)
            ELSE NULL
        END AS reward_id
    FROM http_log 
),
lagged_http_log AS (
    -- Calculate previous timestamp for each user
    SELECT
        user_id,
        "timestamp",
        campaign_id,
        reward_id,
        LAG("timestamp") OVER (PARTITION BY user_id ORDER BY "timestamp") AS prev_timestamp
    FROM parsed_http_log
),
user_sessions AS (
    -- Define user sessions by identifying gaps greater than 5 minutes.
    -- In case of the beginning of the session, which will not have the previous timestamp, the prev_timestamp will be Null
    -- But Null can not be performed in an aggregation with the timestamp column 
    -- To handle the case where prev_timestamp might be NULL for the very first log entry for each user, this query substitutes prev_timestamp with "timestamp" - INTERVAL '6 minutes' 
    -- The interval value (6 minutes) is slightly more than the session gap threshold (5 minutes) to ensure that any log entry with no prior timestamp (because it’s the first entry) will always be treated as the start of a new session.
    SELECT
        user_id,
        "timestamp" AS event_time,
        campaign_id,
        reward_id,
        SUM(session_start) OVER (PARTITION BY user_id ORDER BY "timestamp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM (
        SELECT
            user_id,
            "timestamp",
            campaign_id,
            reward_id,
            CASE 
                WHEN "timestamp" - COALESCE(prev_timestamp, "timestamp" - INTERVAL '6 minutes') > INTERVAL '5 minutes' 
                THEN 1
                ELSE 0
            END AS session_start
        FROM lagged_http_log
    ) subquery
),
session_start_to_end_time AS (
    -- Determine the start and end time of each session 
    select distinct
        user_id,
        session_id,
        MIN(event_time) over (partition by user_id, session_id ) AS session_start,
        MAX(event_time) over (partition by user_id, session_id ) AS session_end,
        campaign_id,
        reward_id
    FROM user_sessions
), 
-- The next 3 CTEs will be used to cross joinning campaigns and rewards within the same session of each user
-- Normally it could be done just by using CROSS JOIN UNNEST an array of campaigns and rewards, but Redshift does not have this function built in 
campaign_session as (
	select 
		user_id,
		session_id, 
		session_start,
		session_end,
		campaign_id
	from session_start_to_end_time 
	where campaign_id is not null
),
reward_session as (
	select 
		user_id,
		session_id, 
		session_start,
		session_end,
		reward_id
	from session_start_to_end_time 
	where reward_id is not null
), 
campaign_reward_cross as (
select 	cs.user_id,
		cs.session_id, 
		cs.session_start,
		cs.session_end,
		cs.campaign_id,
		rs.reward_id
from campaign_session cs
join reward_session rs
on cs.user_id = rs.user_id
and cs.session_id = rs.session_id
),
campaign_reward_check AS (
    -- Look up the mapping table to find any matches with the campaign and rewards
    SELECT
        crc.user_id,
        crc.session_start,
        crc.session_end,
        crc.campaign_id,
        crc.reward_id,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM campaign_reward_mapping crm
                WHERE crm.campaign_id = crc.campaign_id
                AND crm.reward_id = crc.reward_id
            ) THEN 1 
            ELSE 0
        END AS reward_driven_by_campaign_view
    FROM campaign_reward_cross crc 
)
SELECT
    user_id,
    session_start,
    session_end,
    LISTAGG(DISTINCT campaign_id, ',') AS campaigns,
    LISTAGG(DISTINCT reward_id, ',') AS rewards_issued,
    CASE 
	    WHEN MAX(reward_driven_by_campaign_view) = 1 THEN TRUE 
    	ELSE FALSE
    END AS reward_driven_by_campaign_view
FROM campaign_reward_check 
GROUP BY user_id, session_start, session_end 

