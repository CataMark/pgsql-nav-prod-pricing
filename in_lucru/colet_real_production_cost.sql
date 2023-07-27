with orders as (
    select
        a."Status",
        a."No_",
        a."Description",
        a."Source No_",
        sum(b."Quantity") as "Output"
    from nav.tbl_int_production_order as a

    left join nav.tbl_int_item_ledger_entry as b
    on b."Order No_" = a."No_" and b."Item No_" = a."Source No_"

    where a."Source No_" in ('3534L41Z', '3534L42Z', '3534L43Z', '3534L44Z') and
        a."Status" = 4 /* status closed */ and a."No_" != 'FP1403' and
        b."Order Line No_" = 10000 and b."Entry Type" = 6 /* entry type output */ and
        b."Posting Date" >= '2021-05-01'::date

    group by a."Status",
        a."No_",
        a."Description",
        a."Source No_"),
consumption as (
    select
        a."Order No_",
        a."Item No_",
        d."Description" as "Item Name",
        d."Inventory Posting Group",
        e."Description" as "Entry Type",
        -1 * sum(a."Quantity") as "Quantity",
        d."Base Unit of Measure",
        sum(c."Cost") as "Cost"
    from nav.tbl_int_item_ledger_entry as a

    inner join orders as b
    on b."No_" = a."Order No_"

    left join lateral(
        select
            c1."Item Ledger Entry No_",
            -1 * sum(c1."Cost Amount (Actual)") as "Cost"
        from nav.tbl_int_value_entry as c1
        where c1."Item Ledger Entry No_" = a."Entry No_"
        group by c1."Item Ledger Entry No_"
    ) as c
    on true

    left join nav.tbl_int_item as d
    on d."No_" = a."Item No_"

    left join nav.tbl_int_item_entry_type as e
    on a."Entry Type" = e."Entry Type"

    where a."Item No_" != b."Source No_" and a."Item No_" != 'SRV_EXEC' /* consumption and output */

    group by a."Order No_",
        a."Item No_",
        d."Description",
        d."Inventory Posting Group",
        e."Description",
        d."Base Unit of Measure"
),
ore as (
    select
        a."Order No_",
        a."Work Center No_",
        a."No_" as "Machine No_",
        a."Description",
        sum(a."Workers Qty_") as "Work Hours"
    from nav.tbl_int_capacity_ledger_entry as a

    inner join orders as b
    on b."No_" = a."Order No_"

    group by a."Order No_",
        a."Work Center No_",
        a."No_",
        a."Description"
)
select
    *
from (
    select
        'Finished' as "Status",
        a."No_" as "Order No_",
        a."Description" as "Order Name",
        a."Source No_",
        a."Output",
        'MAT_CONSUMPTION' as "Value Type",
        null::text as "Work Center No_",
        b."Item No_",
        b."Item Name",
        b."Inventory Posting Group",
        b."Entry Type",
        b."Quantity",
        b."Base Unit of Measure",
        b."Cost"
    from orders as a

    inner join consumption as b
    on a."No_" = b."Order No_"

    where b."Quantity" != 0

    union all

    select
        'Finished' as "Status",
        a."No_" as "Order No_",
        a."Description" as "Order Name",
        a."Source No_",
        a."Output",
        'WORK_HOURS' as "Value Type",
        b."Work Center No_",
        b."Machine No_" as "Item No_",
        b."Description" as "Item Name",
        null::text as "Inventory Posting Group",
        null::text as "Entry Type",
        b."Work Hours" as "Quantity",
        'H' as "Base Unit of Measure",
        null::decimal as "Cost"
    from orders as a

    inner join ore as b
    on a."No_" = b."Order No_") as x
order by x."Source No_", x."Order No_", x."Value Type", x."Work Center No_", x."Item No_";