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
    FOREIGN KEY (store_id) REFERENCES public.dimStore (id)
    FOREIGN KEY (product_id) REFERENCES public.dimProduct (id)
);

CREATE INDEX IF NOT EXISTS ix_factInventory_store_product_date ON public.factInventory USING btree (store_id, product_id, snapshot_date);
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_factInventory_store'
    ) THEN
        ALTER TABLE public.factInventory
        ADD CONSTRAINT fk_factInventory_store
            FOREIGN KEY (store_id)
            REFERENCES public.dimStore (id);
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE staging.sp_factInventory_v1_publish (IN p_batchid integer)
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.factInventory fi
        INNER JOIN (
            SELECT DISTINCT snapshot_date
            FROM staging.factInventory_v1
            WHERE rowBatchId = p_batchid
        ) src_dates ON fi.snapshot_date = src_dates.snapshot_date
    ) THEN
        RAISE EXCEPTION 'Load failed: data already exists in factInventory for one or more snapshot_date values in batch %.', p_batchid;
    END IF;

    INSERT INTO public.dataqualityerrors (
        table_name,
        row_id,
        error_description,
        rowBatchId
    )
    SELECT 
        'factInventory',
        id,
        CONCAT('Negative stock_on_hand: ', stock_on_hand, ' for store_id: ', store_id, ', product_id: ', product_id),
        rowBatchId
    FROM staging.factInventory_v1
    WHERE rowBatchId = p_batchid
      AND stock_on_hand < 0;
    
    INSERT INTO public.dataqualityerrors (
        table_name,
        row_id,
        error_description,
        rowBatchId
    )
    SELECT 
        'factInventory',
        inv.id,
        CONCAT('Invalid store_id: ', inv.store_id, ' does not exist in dimStore. Setting to -1 (Unknown Store) for product_id: ', inv.product_id),
        inv.rowBatchId
    FROM staging.factInventory_v1 inv
    LEFT JOIN public.dimStore ds ON inv.store_id = ds.id
    WHERE inv.rowBatchId = p_batchid
      AND ds.id IS NULL;
    
    INSERT INTO public.factInventory (
        store_id,
        product_id,
        stock_on_hand,
        snapshot_date,
        rowBatchId
    )
    SELECT 
        CASE 
            WHEN ds.id IS NULL THEN -1  -- Use -1 for unknown stores
            ELSE inv.store_id
        END AS store_id,
        inv.product_id,
        inv.stock_on_hand,
        inv.snapshot_date,
        inv.rowBatchId
    FROM staging.factInventory_v1 inv
    LEFT JOIN public.dimStore ds ON inv.store_id = ds.id
    WHERE inv.rowBatchId = p_batchid;
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
