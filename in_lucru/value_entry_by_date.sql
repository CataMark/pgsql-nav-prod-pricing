select
    a."Entry No_",
    a."Item No_",
    a."Posting Date"::date,
    b."Description" as "Item Ledger Entry Type",
    a."Source No_",
    a."Document No_",
    a."Description",
    a."Location Code",
    a."Inventory Posting Group",
    a."Source Posting Group",
    a."Item Ledger Entry No_",
    a."Valued Quantity",
    a."Invoiced Quantity",
    a."Cost per Unit",
    a."User ID",
    a."Source Code",
    c."Description" as "Source Type",
    a."Sales Amount (Actual)",
    a."Cost Amount (Actual)",
    a."Document Date"::date,
    a."External Document No_",
    d."Description" as "Document Type",
    a."Document Line No_",
    e."Description" as "Order Type",
    a."Order No_",
    a."Order Line No_",
    f."Description" as "Entry Type"
from nav.tbl_int_value_entry as a

left join nav.tbl_int_item_entry_type as b
on a."Item Ledger Entry Type" = b."Entry Type"

left join nav.tbl_int_item_entry_source_type as c
on a."Source Type" = c."Source Type"

left join nav.tbl_int_item_entry_document_type as d
on a."Document Type" = d."Document Type"

left join nav.tbl_int_item_entry_order_type as e
on a."Order Type" = e."Order Type"

left join nav.tbl_int_value_entry_type as f
on a."Entry Type" = f."Entry Type"

where a.mod_de::date = '2022-03-02';