{{ config(
    materialized = 'table',
    unique_key = 'client_id',
    tags = ["cl", "clients"]
) }}

WITH import_il_clients AS (SELECT * FROM {{ source('csv', 'clients') }}),

cleaned AS (

    SELECT

        CAST(client_id AS STRING) AS client_id,
        client_name,

        /* Remove emojis from primary_industry column & added for secondary_industry column just in case */
        REGEXP_REPLACE(primary_industry, r'[\p{Emoji_Presentation}\p{Extended_Pictographic}]', '')      AS primary_industry,
        REGEXP_REPLACE(secondary_industry, r'[\p{Emoji_Presentation}\p{Extended_Pictographic}]', '')    AS secondary_industry,
        country

    FROM import_il_clients
)

SELECT * FROM cleaned
