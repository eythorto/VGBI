CREATE TABLE IF NOT EXISTS staging.factInventory_v1 (
    id bigserial NOT NULL,
    store_id int NOT NULL,
    product_id int NOT NULL,
    stock_on_hand int NOT NULL,

    rowBatchId integer NOT NULL,
    rowCreated timestamptz NOT NULL DEFAULT timezone('utc', now())
);

CREATE TABLE IF NOT EXISTS public.factInventory (
    id bigserial NOT NULL,
    store_id int NOT NULL,
    product_id int NOT NULL,
    snapshot_date date NOT NULL,
    stock_on_hand int NOT NULL,

    rowBatchId integer NOT NULL,
    rowCreated timestamptz NOT NULL DEFAULT timezone('utc', now()),

    CONSTRAINT factInventory_pkey PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS ix_factInventory_store_product_date ON public.factInventory USING btree (store_id, product_id, snapshot_date);

CREATE OR REPLACE PROCEDURE staging.sp_factInventory_v1_publish (IN p_batchid integer)
LANGUAGE plpgsql
AS $$
BEGIN
    /*
    eitt stock_on_hand er neikvætt
    nokkur store_id eru ekki til. setja id sem -1 "Unknown Store"
    */
    INSERT INTO public.factInventory (
        store_id,
        product_id,
        stock_on_hand,
        snapshot_date,
        rowBatchId
    )
    SELECT 
        store_id,
        product_id,
        stock_on_hand,
        snapshot_date,
        rowBatchId
    FROM staging.factInventory_v1
    WHERE rowBatchId = p_batchid;
END;
$$;

CREATE OR REPLACE PROCEDURE staging.sp_factInventory_v1_postprocess (IN p_batchid integer)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE
    FROM staging.factInventory_v1
    WHERE rowBatchId = p_batchid;
END;

$$;
