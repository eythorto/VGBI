CREATE TABLE IF NOT EXISTS staging.dimInventory_v1 (
    id bigserial NOT NULL,
    store_id int NOT NULL,
    product_id int NOT NULL,
    stock_on_hand int NOT NULL,

    rowBatchId integer NOT NULL,
    rowCreated timestamptz NOT NULL DEFAULT timezone('utc', now())
);

CREATE INDEX ix_dimInventory_store_product ON public.dimInventory USING btree (store_id, product_id);

CREATE TABLE IF NOT EXISTS public.dimInventory (
    id bigserial NOT NULL,
    store_id int NOT NULL,
    product_id int NOT NULL,
    stock_on_hand int NOT NULL,

    rowBatchId integer NOT NULL,
    rowCreated timestamptz NOT NULL DEFAULT timezone('utc', now()),
    rowModified timestamptz NOT NULL DEFAULT timezone('utc', now()),

    CONSTRAINT dimInventory_pkey PRIMARY KEY (id),
    CONSTRAINT uq_dimInventory_store_product UNIQUE (store_id, product_id)
);

CREATE OR REPLACE PROCEDURE staging.sp_dimInventory_v1_publish (IN p_batchid integer)
LANGUAGE plpgsql
AS $$
BEGIN

    WITH cte_data AS
    (
        SELECT *
        FROM staging.dimInventory_v1
        WHERE rowBatchId = p_batchid
    )

    MERGE INTO public.dimInventory trg
    USING cte_data src
    ON trg.store_id = src.store_id AND trg.product_id = src.product_id

    WHEN MATCHED THEN
        UPDATE SET
            stock_on_hand = src.stock_on_hand,
            rowModified   = timezone('utc', now()),
            rowBatchId   = p_batchid

    WHEN NOT MATCHED THEN
        INSERT (
            id,
            store_id,
            product_id,
            stock_on_hand,
            rowBatchId
        )
        VALUES (
            src.id,
            src.store_id,
            src.product_id,
            src.stock_on_hand,
            p_batchid
        );
END;
$$;

CREATE OR REPLACE PROCEDURE staging.sp_dimInventory_v1_postprocess (IN p_batchid integer)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE
    FROM staging.dimInventory_v1
    WHERE rowBatchId = p_batchid;
END;

$$;
