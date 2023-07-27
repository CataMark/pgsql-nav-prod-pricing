with a as (
    select
        a1."Location Code",
        a1."Prod_ Order No_",
        a2."Source No_",
        a2."Description" as "Order Name",
        a7."Description" as "Order Status",
        a2."Last Date Modified",
        a1."Line No_",
        a1."Item No_",
        a1."Description" as "Item Name",
        a3."Inventory Posting Group",
        a4."Description" as "Technical Family",
        a6."Description" as "Own Comp_ Flushing Method",
        a1."Quantity" as "Expected Quantity"
    from nav.tbl_int_prod_order_line as a1

    inner join nav.tbl_int_production_order as a2
    on a1."Prod_ Order No_" = a2."No_"

    left join nav.tbl_int_item as a3
    on a1."Item No_" = a3."No_"

    left join nav.tbl_int_technical_family_ as a4
    on a3."Technical Family" = a4."Code"

    inner join lateral(
        select distinct on (a5."Prod_ Order No_", a5."Prod_ Order Line No_")
            a5."Flushing Method"
        from nav.tbl_int_prod_order_component as a5
        where a5."Prod_ Order No_" = a1."Prod_ Order No_" and a5."Prod_ Order Line No_" = a1."Line No_"
        order by a5."Prod_ Order No_" asc, a5."Prod_ Order Line No_" asc, a5."Flushing Method" desc
    ) as a5
    on a5."Flushing Method" != 0

    left join nav.tbl_int_prod_order_component_flushing_method as a6
    on a5."Flushing Method" = a6."Flushing Method"
    
    left join nav.tbl_int_production_order_status as a7
    on a2."Status" = a7."Status"
    
    where a2."Status" = 3)
select
    a.*,
    c."Operation No_" as "Last Operation No_",
    b."Ledger Output",
    d."Last Operation Qty",
    e."Stock per Location"
from a

left join lateral(
    select
        round(sum(b1."Quantity"), 2) as "Ledger Output"
    from nav.tbl_int_item_ledger_entry as b1
    where b1."Order No_" = a."Prod_ Order No_" and b1."Order Line No_" = a."Line No_" and b1."Entry Type" = 6 /* output */
) as b
on true

left join lateral(
    select distinct on (c1."Prod_ Order No_", c1."Routing Reference No_")
        c1."Operation No_"
    from nav.tbl_int_prod_order_routing_line as c1
    where c1."Prod_ Order No_" = a."Prod_ Order No_" and c1."Routing Reference No_" = a."Line No_"
    order by c1."Prod_ Order No_" asc, c1."Routing Reference No_" asc,
            (case when nullif(c1."Next Operation No_", '') is null then 1 else 2 end) asc, c1."Operation No_" desc
) as c
on true

left join lateral(
    select
        round(sum(d1."Output Quantity"), 2) as "Last Operation Qty"
    from nav.tbl_int_capacity_ledger_entry as d1
    where d1."Order No_" = a."Prod_ Order No_" and d1."Order Line No_" = a."Line No_" and d1."Operation No_" = c."Operation No_"
) as d
on true

left join lateral(
    select
        round(sum(e1."Quantity"), 2) as "Stock per Location"
    from nav.tbl_int_item_ledger_entry as e1
    where a."Item No_" = e1."Item No_" and a."Location Code" = e1."Location Code"
) as e
on true

where coalesce(b."Ledger Output", 0) - coalesce(d."Last Operation Qty", 0) != 0

order by a."Prod_ Order No_",
        a."Line No_",
        c."Operation No_";