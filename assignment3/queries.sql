-- Query sýnir fjölda sem seldur er eftir degi niður á búð og vöru
SELECT
    dc.date,
    fs.store_id,
    fs.product_id,
    SUM(fs.units_sold) AS units_sold
FROM factSales fs
JOIN dimCalendar dc ON fs.calendar_id = dc.id
GROUP BY dc.date, fs.store_id, fs.product_id
ORDER BY dc.date;

-- Query sýnir fjölda sem seldur er eftir viku niður á búð og vöru
SELECT
    dc.year,
    dc.weekno,
    fs.store_id,
    fs.product_id,
    SUM(fs.units_sold) AS units_sold
FROM factSales fs
JOIN dimCalendar dc ON fs.calendar_id = dc.id
GROUP BY dc.year, dc.weekno, fs.store_id, fs.product_id
ORDER BY dc.year, dc.weekno;

-- Query sýnir fjölda sem seldur er eftir mánuði niður á búð og vöru
SELECT
    dc.year,
    dc.monthno,
    fs.store_id,
    fs.product_id,
    SUM(fs.units_sold) AS units_sold
FROM factSales fs
JOIN dimCalendar dc ON fs.calendar_id = dc.id
GROUP BY dc.year, dc.monthno, fs.store_id, fs.product_id
ORDER BY dc.year, dc.monthno;

-- Query sýnir fjölda sem seldur er eftir ári niður á búð og vöru
SELECT
    dc.year,
    fs.store_id,
    fs.product_id,
    SUM(fs.units_sold) AS units_sold
FROM factSales fs
JOIN dimCalendar dc ON fs.calendar_id = dc.id
GROUP BY dc.year, fs.store_id, fs.product_id
ORDER BY dc.year;

-- Skoða veltu og kostnað með sama niðurbroti
SELECT 
    c.year, 
    c.monthno,
    s.name,
    p.name,
    SUM(fs.amount_sold) AS revenue, 
    SUM(fs.amount_cost) AS cost,
    SUM(fs.profit) AS profit
FROM public.factSales fs
JOIN public.dimCalendar c ON fs.calendar_id = c.id
JOIN public.dimStore s ON fs.store_id = s.id
JOIN public.dimProduct p ON fs.product_id = p.id
GROUP BY c.year, c.monthno, s.name, p.name
ORDER BY c.year, c.monthno;


-- Meðalveltu, meðal upphæð kröfu og meðal fjölda keyptra hluta per körfu
SELECT 
    ROUND(AVG(revenue), 5) AS average_revenue,
    ROUND(AVG(cost), 5) AS average_cost_per_basket,
    ROUND(AVG(total_items), 5) AS average_number_of_products_per_basket
FROM (
    SELECT
        receipt,
        SUM(amount_sold) AS revenue,
        SUM(amount_cost) AS cost,
        SUM(units_sold) AS total_items
    FROM public.factSales
    GROUP BY receipt
) AS basket;

-- Lager upplýsingar niður á búð og vöru
SELECT DISTINCT 
    s.id AS store_id,
    s.name AS store_name,
    stock_on_hand, 
    p.id AS product_id, 
    p.name AS product_name, 
    snapshot_date
FROM public.factInventory fi
JOIN public.dimStore s ON fi.store_id = s.id
JOIN public.dimProduct p ON fi.product_id = p.id
ORDER BY snapshot_date, s.id, p.id;

