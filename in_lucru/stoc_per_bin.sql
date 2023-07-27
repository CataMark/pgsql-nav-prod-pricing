select
    *
from (
    select
        a."Item No_",
        b."Description",
        b."Inventory Posting Group",
        a."Location Code",
        a."Bin Code",
        a."Lot No_",
        a."Unit of Measure Code",
        b."Base Unit of Measure",
        b."Unit Cost",
        round(sum(a."Quantity"), 3) as "Quantity"
    from nav.tbl_int_warehouse_entry as a

    left join nav.tbl_int_item as b
    on a."Item No_" = b."No_"

    /* where a."Location Code" = 'AIT' and (a."Bin Code" like 'DEP%' or a."Bin Code" like 'FER%') */
    group by a."Item No_",
            b."Description",
            b."Inventory Posting Group",
            a."Location Code",
            a."Bin Code",
            a."Lot No_",
            a."Unit of Measure Code",
            b."Base Unit of Measure",
            b."Unit Cost") as x
where x."Quantity" != 0;