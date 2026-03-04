CREATE TABLE IF NOT EXISTS staging.dimStore_v1
(
    id integer NOT NULL,
    name varchar(100) NOT NULL,
    city varchar(100) NOT NULL,
    country varchar(100) NOT NULL,
    location varchar(100) NOT NULL,
    open_date date NOT NULL,

    rowBatchId integer NOT NULL,
    rowCreated timestamptz NOT NULL DEFAULT timezone('utc', now())
);

CREATE TABLE IF NOT EXISTS public.dimStore
(
    id integer not null,
    name varchar(100) NOT NULL,
    city varchar(100) NOT NULL,
    country varchar(100) NOT NULL,
    location varchar(100) NOT NULL,
    open_date date NOT NULL,

    rowBatchId integer NOT NULL,
    rowCreated timestamptz NOT NULL DEFAULT timezone('utc', now()),
    rowModified timestamptz NOT NULL DEFAULT timezone('utc', now()),

    CONSTRAINT uq_dimStore_id UNIQUE (id)
);

CREATE OR REPLACE PROCEDURE staging.sp_dimStore_v1_publish (IN p_batchid integer)
LANGUAGE plpgsql
AS $$
BEGIN

    WITH cte_data AS
    (
        SELECT *
        FROM staging.dimStore_v1
        WHERE rowBatchId = p_batchid
    )

    MERGE INTO public.dimStore trg
    USING cte_data src
    ON trg.id = src.id

    WHEN MATCHED THEN
        UPDATE SET
            name        = src.name,
            city        = src.city,
            country     = src.country,
            location    = src.location,
            open_date   = src.open_date,
            rowModified = timezone('utc', now()),
            rowBatchId  = p_batchid

    WHEN NOT MATCHED THEN
        INSERT (
            id,
            name,
            city,
            country,
            location,
            open_date,
            rowBatchId
        )
        VALUES (
            src.id,
            src.name,
            src.city,
            src.country,
            src.location,
            src.open_date,
            p_batchid
        );

END;
$$;

CREATE OR REPLACE PROCEDURE staging.sp_dimStore_v1_postprocess (IN p_batchid integer)
LANGUAGE plpgsql
AS $$
BEGIN

    DELETE
    FROM staging.dimStore_v1
    WHERE rowBatchId = p_batchid;

END;
$$;