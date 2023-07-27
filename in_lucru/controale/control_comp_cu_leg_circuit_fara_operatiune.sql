select
    c."Location Code",
    'Released' as "Order Status",
    a."Prod_ Order No_",
    b."Source No_",
    b."Description" as "Order Name",
    a."Prod_ Order Line No_",
    c."Item No_" as "Item (OF)",
    c."Description" as "Item (OF) Name",
    round(c."Finished Quantity", 2) as "Item (OF) Finished Qty_",
    a."Line No_" as "Comp_ Line No_",
    a."Item No_",
    a."Description" as "Item Name",
    e."Inventory Posting Group" as "Item Type",
    f."Description" as "Technical Family",
    g."Description" as "Flushing Method",
    c."Routing No_",
    c."Routing Version Code",
    a."Routing Link Code",
    (j.pozitii > 0) as "IN_BOM",
    e."Base Unit of Measure",
    round(coalesce(-1 * sum(h."Quantity"), 0), 2) as "Actual Consumption"
from nav.tbl_int_prod_order_component as a

left join nav.tbl_int_production_order as b
on a."Prod_ Order No_" = b."No_"

left join nav.tbl_int_prod_order_line as c
on a."Prod_ Order No_" = c."Prod_ Order No_" and a."Prod_ Order Line No_" = c."Line No_"

left join nav.tbl_int_prod_order_routing_line as d
on a."Prod_ Order No_" = d."Prod_ Order No_" and a."Prod_ Order Line No_" = d."Routing Reference No_" and a."Routing Link Code" = d."Routing Link Code"

left join nav.tbl_int_item as e
on a."Item No_" = e."No_"

left join nav.tbl_int_technical_family_ as f
on e."Technical Family" = f."Code"

left join nav.tbl_int_prod_order_component_flushing_method as g
on a."Flushing Method" = g."Flushing Method"

left join nav.tbl_int_item_ledger_entry as h
on a."Prod_ Order No_" = h."Order No_" and a."Prod_ Order Line No_" = h."Order Line No_" and a."Line No_" = h."Prod_ Order Comp_ Line No_"

left join lateral (
    select
        count(*) as pozitii
    from nav.tbl_int_routing_line as j1
    where j1."Routing No_" = c."Routing No_" and j1."Version Code" = c."Routing Version Code" and j1."Routing Link Code" = a."Routing Link Code") as j
    on true

where a."Status" = 3 and a."Flushing Method" != 0 and coalesce(h."Entry Type", -1) in (-1, 5) and d."Operation No_" is null

group by c."Location Code",
        a."Prod_ Order No_",
        b."Source No_",
        b."Description",
        a."Prod_ Order Line No_",
        c."Item No_",
        c."Description",
        c."Finished Quantity",
        a."Line No_",
        a."Item No_",
        a."Description",
        e."Inventory Posting Group",
        f."Description",
        g."Description",
        c."Routing No_",
        c."Routing Version Code",
        a."Routing Link Code",
        (j.pozitii > 0),
        e."Base Unit of Measure";