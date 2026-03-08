CREATE TABLE IF NOT EXISTS staging.factSales_v1 (
    id BIGINT,
    store_id INT NOT NULL,
    product_id INT NOT NULL,
    receipt VARCHAR(100) NOT NULL,
    units_sold INT NOT NULL,
    date DATE NOT NULL,

    rowBatchId INT NOT NULL,
    rowCreated TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
);

CREATE TABLE IF NOT EXISTS public.factSales (
    id BIGINT PRIMARY KEY,
    calendar_id INT NOT NULL,
    store_id INT NOT NULL,
    product_id INT NOT NULL,
    receipt VARCHAR(100) NOT NULL,
    units_sold INT NOT NULL,

    amount_sold NUMERIC(10,2),
    amount_cost NUMERIC(10,2),
    profit NUMERIC(10,2),

    rowBatchId INT NOT NULL,
    rowCreated TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()),

    FOREIGN KEY (calendar_id) REFERENCES public.dimCalendar(id),
    FOREIGN KEY (store_id) REFERENCES public.dimStore(id),
    FOREIGN KEY (product_id) REFERENCES public.dimProduct(id)
);

CREATE OR REPLACE PROCEDURE staging.sp_factSales_v1_publish (IN p_batchid integer)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.dataqualityerrors (
        table_name,
        row_id,
        error_description,
        rowBatchId
    )
    SELECT 
        'factSales',
        id,
        CONCAT('Negative units_sold: ', units_sold, ' for store_id: ', store_id, ', product_id: ', product_id),
        rowBatchId
    FROM staging.factSales_v1
    WHERE rowBatchId = p_batchid
      AND units_sold < 0;
    INSERT INTO public.dataqualityerrors (
        table_name,
        row_id,
        error_description,
        rowBatchId
    )    SELECT 
        'factSales',
        fs.id,
        CONCAT('Invalid store_id: ', fs.store_id, ' does not exist in dimStore. Setting to -1 (Unknown Store) for product_id: ', fs.product_id),
        fs.rowBatchId
    FROM staging.factSales_v1 fs
    LEFT JOIN public.dimStore ds ON fs.store_id = ds.id
    WHERE fs.rowBatchId = p_batchid
      AND ds.id IS NULL; 

    INSERT INTO public.factSales (
        id,
        calendar_id,
        store_id,
        product_id,
        receipt,
        units_sold,
        amount_sold,
        amount_cost,
        profit,
        rowBatchId,
        rowCreated
    )
    SELECT
        fs.id,
        (SELECT id FROM public.dimCalendar WHERE date = fs.date) AS calendar_id,
        COALESCE(ds.id, -1) AS store_id,
        fs.product_id,
        fs.receipt,
        fs.units_sold,
        fs.units_sold * COALESCE(dp.price, 0) AS amount_sold,
        fs.units_sold * COALESCE(dp.cost, 0) AS amount_cost,
        (fs.units_sold * COALESCE(dp.price, 0)) - (fs.units_sold * COALESCE(dp.cost, 0)) AS profit,
        fs.rowBatchId,
        fs.rowCreated
    FROM staging.factSales_v1 fs
    JOIN public.dimProduct dp ON fs.product_id = dp.id
    JOIN public.dimStore ds ON fs.store_id = ds.id
    WHERE fs.rowBatchId = p_batchid;
END;
$$;

CREATE OR REPLACE PROCEDURE staging.sp_factSales_v1_postprocess (IN p_batchid integer)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE
    FROM staging.factSales_v1
    WHERE rowBatchId = p_batchid;
END;
$$;