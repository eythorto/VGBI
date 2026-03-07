CREATE TABLE IF NOT EXISTS staging.dimProduct_v1
(
    id integer NOT NULL,
    name varchar(100),
    category varchar(100),
    cost varchar(100),
    price varchar(100),
    seasonality_flag varchar(100),
    freshness_days integer,
    shelf_life_days integer,

    rowBatchId integer NOT NULL,
    rowCreated timestamptz NOT NULL DEFAULT timezone('utc', now())
);

CREATE TABLE IF NOT EXISTS public.dimProduct
(
    id integer NOT NULL,
    name varchar(100) NOT NULL,
    category varchar(100) NOT NULL,
    cost NUMERIC(10,2),
    price NUMERIC(10,2),
    currency VARCHAR,
    seasonality_flag varchar(100) NOT NULL,
    freshness_days integer NOT NULL,
    shelf_life_days integer NOT NULL,

    rowBatchId integer NOT NULL,
    rowCreated timestamptz NOT NULL DEFAULT timezone('utc', now()),
    rowModified timestamptz NOT NULL DEFAULT timezone('utc', now()),

    CONSTRAINT uq_dimProduct_id UNIQUE (id)
);

CREATE OR REPLACE PROCEDURE staging.sp_dimProduct_v1_publish (IN p_batchid integer)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log any cost values that fail to convert to decimal
    INSERT INTO public.dataqualityerrors (table_name, row_id, error_description, rowBatchId)
    SELECT 
        'dimProduct' AS table_name,
        id AS row_id,
        'Cost failed to convert to decimal: ' || COALESCE(cost, 'NULL') AS error_description,
        p_batchid
    FROM staging.dimProduct_v1
    WHERE rowBatchId = p_batchid
      AND CAST(NULLIF(REGEXP_REPLACE(cost, '[^0-9.-]', '', 'g'), '') AS NUMERIC(10,2)) IS NULL;

    -- Log any price values that fail to convert to decimal
    INSERT INTO public.dataqualityerrors (table_name, row_id, error_description, rowBatchId)
    SELECT 
        'dimProduct' AS table_name,
        id AS row_id,
        'Price failed to convert to decimal: ' || COALESCE(price, 'NULL') AS error_description,
        p_batchid
    FROM staging.dimProduct_v1
    WHERE rowBatchId = p_batchid
      AND CAST(NULLIF(REGEXP_REPLACE(price, '[^0-9.-]', '', 'g'), '') AS NUMERIC(10,2)) IS NULL;

    -- Log negative costs
    INSERT INTO public.dataqualityerrors (table_name, row_id, error_description, rowBatchId)
    SELECT 
        'dimProduct' AS table_name,
        id AS row_id,
        'Cost is negative: ' || cost AS error_description,
        p_batchid
    FROM staging.dimProduct_v1
    WHERE rowBatchId = p_batchid
      AND CAST(NULLIF(REGEXP_REPLACE(cost, '[^0-9.-]', '', 'g'), '') AS NUMERIC(10,2)) < 0;

    -- Log negative prices
    INSERT INTO public.dataqualityerrors (table_name, row_id, error_description, rowBatchId)
    SELECT 
        'dimProduct' AS table_name,
        id AS row_id,
        'Price is negative: ' || price AS error_description,
        p_batchid
    FROM staging.dimProduct_v1
    WHERE rowBatchId = p_batchid
      AND CAST(NULLIF(REGEXP_REPLACE(price, '[^0-9.-]', '', 'g'), '') AS NUMERIC(10,2)) < 0;

    WITH cte_data AS
    (
        SELECT 
            id,
            name,
            category,
            CASE 
                WHEN CAST(NULLIF(REGEXP_REPLACE(cost, '[^0-9.-]', '', 'g'), '') AS NUMERIC(10,2)) < 0 
                THEN NULL
                ELSE CAST(NULLIF(REGEXP_REPLACE(cost, '[^0-9.-]', '', 'g'), '') AS NUMERIC(10,2))
            END AS cost,
            CASE 
                WHEN CAST(NULLIF(REGEXP_REPLACE(price, '[^0-9.-]', '', 'g'), '') AS NUMERIC(10,2)) < 0 
                THEN NULL
                ELSE CAST(NULLIF(REGEXP_REPLACE(price, '[^0-9.-]', '', 'g'), '') AS NUMERIC(10,2))
            END AS price,
            REGEXP_REPLACE(COALESCE(cost, price, ''), '[0-9. ]', '', 'g') AS currency,
            seasonality_flag,
            freshness_days,
            shelf_life_days,
            rowBatchId
        FROM staging.dimProduct_v1
        WHERE rowBatchId = p_batchid
    )

    MERGE INTO public.dimProduct trg
    USING cte_data src
    ON trg.id = src.id

    WHEN MATCHED THEN
        UPDATE SET
            name              = src.name,
            category          = src.category,
            cost              = src.cost,
            price             = src.price,
            currency          = src.currency,
            seasonality_flag  = src.seasonality_flag,
            freshness_days    = src.freshness_days,
            shelf_life_days   = src.shelf_life_days,
            rowModified       = timezone('utc', now()),
            rowBatchId        = p_batchid

    WHEN NOT MATCHED THEN
        INSERT (
            id,
            name,
            category,
            cost,
            price,
            currency,
            seasonality_flag,
            freshness_days,
            shelf_life_days,
            rowBatchId
        )
        VALUES (
            src.id,
            src.name,
            src.category,
            src.cost,
            src.price,
            src.currency,
            src.seasonality_flag,
            src.freshness_days,
            src.shelf_life_days,
            p_batchid
        );

END;
$$;

CREATE OR REPLACE PROCEDURE staging.sp_dimProduct_v1_postprocess (IN p_batchid integer)
LANGUAGE plpgsql
AS $$
BEGIN

    DELETE
    FROM staging.dimProduct_v1
    WHERE rowBatchId = p_batchid;

END;
$$;