select
    c."Location Code",
    'Released' as "Order Status",
    a."Prod_ Order No_",
    b."Source No_",
    b."Description" as "Order Name",
    a."Prod_ Order Line No_",
    c."Item No_" as "Item (OF)",
    c."Description" as "Item (OF) Name",
    a."Line No_" as "Comp_ Line No_",
    a."Item No_",
    a."Description" as "Item Name",
    d."Inventory Posting Group" as "Item Type",
    e."Description" as "Technical Family",
    f."Description" as "Flushing Method",
    c."Routing No_",
    c."Routing Version Code",
    round(c."Finished Quantity", 2) as "Item (OF) Finished Qty_",
    d."Base Unit of Measure",
    round(coalesce(-1 * sum(g."Quantity"), 0), 2) as "Actual Consumption"
from nav.tbl_int_prod_order_component as a

left join nav.tbl_int_production_order as b
on a."Prod_ Order No_" = b."No_"

left join nav.tbl_int_prod_order_line as c
on a."Prod_ Order No_" = c."Prod_ Order No_" and a."Prod_ Order Line No_" = c."Line No_"

left join nav.tbl_int_item as d
on a."Item No_" = d."No_"

left join nav.tbl_int_technical_family_ as e
on d."Technical Family" = e."Code"

left join nav.tbl_int_prod_order_component_flushing_method as f
on a."Flushing Method" = f."Flushing Method"

left join nav.tbl_int_item_ledger_entry as g
on a."Prod_ Order No_" = g."Order No_" and a."Prod_ Order Line No_" = g."Order Line No_" and a."Line No_" = g."Prod_ Order Comp_ Line No_"

where a."Status" = 3 and a."Flushing Method" != 0 and coalesce(g."Entry Type", -1) in (-1, 5) and nullif(a."Routing Link Code", '') is null

group by c."Location Code",
    a."Prod_ Order No_",
    b."Source No_",
    b."Description",
    a."Prod_ Order Line No_",
    c."Item No_",
    c."Description",
    a."Line No_",
    a."Item No_",
    a."Description",
    d."Inventory Posting Group",
    e."Description",
    f."Description",
    c."Routing No_",
    c."Routing Version Code",
    c."Finished Quantity",
    d."Base Unit of Measure";