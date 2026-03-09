-- Dim Calendar v_2
CREATE OR REPLACE PROCEDURE public.sp_dimCalendar_generate(IN p_batchid integer)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.dimCalendar (
        id,
        date,
        year,
        monthNo,
        weekNo,
        yyyy_mm,
        yyyy_ww,
        dayofMonth,
        dayofYear,
        rowBatchId,
        rowCreated,
        rowModified
    )
    SELECT
        TO_CHAR(d, 'YYYYMMDD')::INT AS id,
        d AS date,
        EXTRACT(YEAR FROM d),
        EXTRACT(MONTH FROM d),
        EXTRACT(WEEK FROM d),
        TO_CHAR(d,'YYYY-MM'),
        TO_CHAR(d,'IYYY-IW'),
        EXTRACT(DAY FROM d),
        EXTRACT(DOY FROM d),
        p_batchid,
        timezone('utc', now()),
        timezone('utc', now())
    FROM generate_series(
        (SELECT COALESCE(MAX(date), DATE '2016-12-31') + INTERVAL '1 day' FROM public.dimCalendar),
        CURRENT_DATE,
        INTERVAL '1 day'
    ) AS d
    ON CONFLICT (id) DO NOTHING;
END;
$$;
