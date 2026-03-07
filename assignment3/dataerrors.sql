-- Generic data quality errors table
DROP TABLE IF EXISTS public.dataqualityerrors;
CREATE TABLE IF NOT EXISTS public.dataqualityerrors
(
    error_id SERIAL PRIMARY KEY,
    table_name varchar(100) NOT NULL,
    row_id integer NOT NULL,
    error_description text,
    rowBatchId integer NOT NULL,
    rowCreated timestamptz NOT NULL DEFAULT timezone('utc', now())
);
