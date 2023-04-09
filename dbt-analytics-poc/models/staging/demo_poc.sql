{{ config(
	materialized='incremental',
    schema="int_marketing",
	table="email"
) }}
SELECT
	date_1 AS activity_date,
	click_date AS activity_datetime,
	bounce_reason AS bounce_reason,
	campaign_id AS campaign_id,
	tactic_id AS campaign_tactic_id,
	platform AS device_platform_type,
	email AS email_from_address,
	subject AS email_subject,
	francise AS franchise,
	country AS hcp_country,
	hcp_id AS hcp_customer_id,
	first_name AS hcp_first_name,
	last_name AS hcp_last_name,
	npi_number AS hcp_npi_number,
	zip AS hcp_postal_code,
	state_1 AS hcp_state,
	hcp_type AS hcp_type,
	'FALSE' AS is_deleted,
	brand AS product_brand,
	record_id AS record_id,
	vendor_name AS vendor_name,
	SENT AS ACTIVITY_VALUE,
	'Sent' AS ACTIVITY_TYPE
FROM
	staging.dmt_datamax_email
WHERE
	sent > 0
UNION ALL
SELECT
	date_1 AS activity_date,
	click_date AS activity_datetime,
	bounce_reason AS bounce_reason,
	campaign_id AS campaign_id,
	tactic_id AS campaign_tactic_id,
	platform AS device_platform_type,
	email AS email_from_address,
	subject AS email_subject,
	francise AS franchise,
	country AS hcp_country,
	hcp_id AS hcp_customer_id,
	first_name AS hcp_first_name,
	last_name AS hcp_last_name,
	npi_number AS hcp_npi_number,
	zip AS hcp_postal_code,
	state_1 AS hcp_state,
	hcp_type AS hcp_type,
	'FALSE' AS is_deleted,
	brand AS product_brand,
	record_id AS record_id,
	vendor_name AS vendor_name,
	DELIVERED AS ACTIVITY_VALUE,
	'Delivered' AS ACTIVITY_TYPE
FROM
	staging.dmt_datamax_email
WHERE
	delivered > 0
UNION ALL
SELECT
	date_1 AS activity_date,
	click_date AS activity_datetime,
	bounce_reason AS bounce_reason,
	campaign_id AS campaign_id,
	tactic_id AS campaign_tactic_id,
	platform AS device_platform_type,
	email AS email_from_address,
	subject AS email_subject,
	francise AS franchise,
	country AS hcp_country,
	hcp_id AS hcp_customer_id,
	first_name AS hcp_first_name,
	last_name AS hcp_last_name,
	npi_number AS hcp_npi_number,
	zip AS hcp_postal_code,
	state_1 AS hcp_state,
	hcp_type AS hcp_type,
	'FALSE' AS is_deleted,
	brand AS product_brand,
	record_id AS record_id,
	vendor_name AS vendor_name,
	OPENS AS ACTIVITY_VALUE,
	'Open' AS ACTIVITY_TYPE
FROM
	staging.dmt_datamax_email
WHERE
	opens > 0
UNION ALL
SELECT
	date_1 AS activity_date,
	click_date AS activity_datetime,
	bounce_reason AS bounce_reason,
	campaign_id AS campaign_id,
	tactic_id AS campaign_tactic_id,
	platform AS device_platform_type,
	email AS email_from_address,
	subject AS email_subject,
	francise AS franchise,
	country AS hcp_country,
	hcp_id AS hcp_customer_id,
	first_name AS hcp_first_name,
	last_name AS hcp_last_name,
	npi_number AS hcp_npi_number,
	zip AS hcp_postal_code,
	state_1 AS hcp_state,
	hcp_type AS hcp_type,
	'FALSE' AS is_deleted,
	brand AS product_brand,
	record_id AS record_id,
	vendor_name AS vendor_name,
	DELETES AS ACTIVITY_VALUE,
	'Deleted' AS ACTIVITY_TYPE
FROM
	staging.dmt_datamax_email
WHERE
	deletes > 0
UNION ALL
SELECT
	date_1 AS activity_date,
	click_date AS activity_datetime,
	bounce_reason AS bounce_reason,
	campaign_id AS campaign_id,
	tactic_id AS campaign_tactic_id,
	platform AS device_platform_type,
	email AS email_from_address,
	subject AS email_subject,
	francise AS franchise,
	country AS hcp_country,
	hcp_id AS hcp_customer_id,
	first_name AS hcp_first_name,
	last_name AS hcp_last_name,
	npi_number AS hcp_npi_number,
	zip AS hcp_postal_code,
	state_1 AS hcp_state,
	hcp_type AS hcp_type,
	'FALSE' AS is_deleted,
	brand AS product_brand,
	record_id AS record_id,
	vendor_name AS vendor_name,
	CLICKS AS ACTIVITY_VALUE,
	'Click' AS ACTIVITY_TYPE
FROM
	staging.dmt_datamax_email
WHERE
	clicks > 0
UNION ALL
SELECT
	date_1 AS activity_date,
	click_date AS activity_datetime,
	bounce_reason AS bounce_reason,
	campaign_id AS campaign_id,
	tactic_id AS campaign_tactic_id,
	platform AS device_platform_type,
	email AS email_from_address,
	subject AS email_subject,
	francise AS franchise,
	country AS hcp_country,
	hcp_id AS hcp_customer_id,
	first_name AS hcp_first_name,
	last_name AS hcp_last_name,
	npi_number AS hcp_npi_number,
	zip AS hcp_postal_code,
	state_1 AS hcp_state,
	hcp_type AS hcp_type,
	'FALSE' AS is_deleted,
	brand AS product_brand,
	record_id AS record_id,
	vendor_name AS vendor_name,
	ENGAGED_VISIT AS ACTIVITY_VALUE,
	'Engage' AS ACTIVITY_TYPE
FROM
	staging.dmt_datamax_email
WHERE
	engaged_visit > 0
UNION ALL
SELECT
	date_1 AS activity_date,
	click_date AS activity_datetime,
	bounce_reason AS bounce_reason,
	campaign_id AS campaign_id,
	tactic_id AS campaign_tactic_id,
	platform AS device_platform_type,
	email AS email_from_address,
	subject AS email_subject,
	francise AS franchise,
	country AS hcp_country,
	hcp_id AS hcp_customer_id,
	first_name AS hcp_first_name,
	last_name AS hcp_last_name,
	npi_number AS hcp_npi_number,
	zip AS hcp_postal_code,
	state_1 AS hcp_state,
	hcp_type AS hcp_type,
	'FALSE' AS is_deleted,
	brand AS product_brand,
	record_id AS record_id,
	vendor_name AS vendor_name,
	FORWARDS AS ACTIVITY_VALUE,
	'Forward' AS ACTIVITY_TYPE
FROM
	staging.dmt_datamax_email
WHERE
	forwards > 0
UNION ALL
SELECT
	date_1 AS activity_date,
	click_date AS activity_datetime,
	bounce_reason AS bounce_reason,
	campaign_id AS campaign_id,
	tactic_id AS campaign_tactic_id,
	platform AS device_platform_type,
	email AS email_from_address,
	subject AS email_subject,
	francise AS franchise,
	country AS hcp_country,
	hcp_id AS hcp_customer_id,
	first_name AS hcp_first_name,
	last_name AS hcp_last_name,
	npi_number AS hcp_npi_number,
	zip AS hcp_postal_code,
	state_1 AS hcp_state,
	hcp_type AS hcp_type,
	'FALSE' AS is_deleted,
	brand AS product_brand,
	record_id AS record_id,
	vendor_name AS vendor_name,
	BOUNCE AS ACTIVITY_VALUE,
	'Bounced' AS ACTIVITY_TYPE
FROM
	staging.dmt_datamax_email
WHERE
	bounce > 0