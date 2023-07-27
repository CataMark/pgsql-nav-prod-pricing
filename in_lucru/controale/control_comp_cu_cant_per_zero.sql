with recursive cte as (
    select
        a."Production BOM No_",
        a."Version Code",
        b."Location Code",
        a."Type",
        a."Line No_"::text,
        a."No_" as "Item No_",
        a."Description",
        a."Quantity per",
        a."Unit of Measure Code"
    from nav.tbl_int_prod_bom_line as a

    inner join (
        select distinct
            b1."Production BOM No_",
            b1."Production BOM Version Code",
            b1."Location Code"
        from nav.tbl_int_prod_order_line as b1
        where b1."Status" = 3
    ) as b
    on a."Production BOM No_" = b."Production BOM No_" and a."Version Code" = b."Production BOM Version Code"

    union all

    select
        a."Production BOM No_",
        a."Version Code",
        a."Location Code",
        a."Type",
        concat(a."Line No_", '_', c."Line No_") as "Line No_",
        c."No_" as "Item No_",
        c."Description",
        (a."Quantity per" * c."Quantity per")::decimal(38,20) as "Quantity per",
        c."Unit of Measure Code"
    from cte as a

    inner join lateral(
        select distinct on (b1."Production BOM No_")
            b1."Production BOM No_",
            b1."Version Code"
        from nav.tbl_int_prod_bom_line as b1
        left join nav.tbl_int_prod_bom_vers as b2
        on b1."Production BOM No_" = b2."Production BOM No_" and b1."Version Code" = b2."Version Code"

        where a."Type" = 2 and a."Item No_" = b1."Production BOM No_"

        order by b1."Production BOM No_",
                (case b2."Location Code" when a."Location Code" then 1 else
                    case when b2."Location Code" is null then 2 else 3 end
                end) asc
    ) as b
    on true

    inner join nav.tbl_int_prod_bom_line as c
    on b."Production BOM No_" = c."Production BOM No_" and coalesce(b."Version Code", '_null') = coalesce(c."Version Code", '_null')
)
select
    'Released' as "Order status",
    a."Prod_ Order No_",
    b."Source No_",
    b."Description" as "Order Name",
    a."Prod_ Order Line No_",
    d."Item No_" as "Item (OF)",
    d."Description" as "Item (OF) Name",
    round(d."Finished Quantity", 2) as "Item (OF) Finished Qty_",
    a."Line No_" as "Comp_ Line No_",
    a."Item No_",
    a."Description" as "Item Name",
    c."Inventory Posting Group" as "Item Type",
    j."Description" as "Technical Family",
    k."Description" as "Flushing Method",
    a."Routing Link Code",
    e."Work Center No_",
    round(coalesce(sum(f."Output Quantity"), 0), 2) as "Declar_ Output Quantity",
    round(coalesce(sum(f."Scrap Quantity"), 0), 2) as "Declar_  Scrap Quantity",
    a."BOM Quantity per" as "Order BOM Qty per",
    a."Unit of Measure Code" as "Order UNMAS",
    h."Quantity per" as "BOM Qty per",
    round(coalesce(sum((f."Output Quantity" + f."Scrap Quantity") * coalesce(a."BOM Quantity per", h."Quantity per")), 0), 2) as "Expected Consumption",
    h."Unit of Measure Code" as "BOM UNMAS",
    round(coalesce(-1 * sum(g."Quantity"/ g."Qty_ per Unit of Measure"), 0), 2) as "Actual Consumption",
    g."Unit of Measure Code" as "Consumption UNMAS"
from nav.tbl_int_prod_order_component as a

left join nav.tbl_int_production_order as b
on a."Prod_ Order No_" = b."No_"

left join nav.tbl_int_item as c
on a."Item No_" = c."No_"

left join nav.tbl_int_prod_order_line as d
on a."Prod_ Order No_" = d."Prod_ Order No_" and a."Prod_ Order Line No_" = d."Line No_"

left join nav.tbl_int_prod_order_routing_line as e
on a."Prod_ Order No_" = e."Prod_ Order No_" and a."Prod_ Order Line No_" = e."Routing Reference No_" and a."Routing Link Code" = e."Routing Link Code"

left join nav.tbl_int_capacity_ledger_entry as f
on e."Prod_ Order No_" = f."Order No_" and e."Routing Reference No_" = f."Order Line No_" and e."Operation No_" = f."Operation No_"

left join nav.tbl_int_item_ledger_entry as g
on a."Prod_ Order No_" = g."Order No_" and a."Prod_ Order Line No_" = g."Order Line No_" and a."Line No_" = g."Prod_ Order Comp_ Line No_"

left join cte as h
on d."Production BOM No_" = h."Production BOM No_" and coalesce(nullif(d."Production BOM Version Code", ''), '_null') = coalesce(nullif(h."Version Code", ''), '_null') and a."Item No_" = h."Item No_"

left join nav.tbl_int_technical_family_ as j
on c."Technical Family" = j."Code"

left join nav.tbl_int_prod_order_component_flushing_method as k
on a."Flushing Method" = k."Flushing Method"

where a."Status" = 3 and a."Flushing Method" != 0 and a."Quantity per" = 0 and coalesce(g."Entry Type", -1) in (-1, 5) 

group by a."Prod_ Order No_",
        b."Source No_",
        b."Description",
        a."Prod_ Order Line No_",
        d."Item No_",
        d."Description",
        d."Finished Quantity",
        a."Line No_",
        a."Item No_",
        a."Description",
        c."Inventory Posting Group",
        j."Description",
        k."Description",
        a."Routing Link Code",
        e."Work Center No_",
        a."BOM Quantity per",
        a."Unit of Measure Code",
        h."Quantity per",
        h."Unit of Measure Code",
        g."Unit of Measure Code";