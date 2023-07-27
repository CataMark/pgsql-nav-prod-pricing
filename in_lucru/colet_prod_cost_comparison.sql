with pfs as (
    select distinct
        a."Order No_",
        a."Item No_",
        b."Description" as "Item Name",
        b."Range",
        c."Location Code"
    from nav.tbl_int_item_ledger_entry as a

    inner join nav.tbl_int_item as b
    on a."Item No_" = b."No_"

    inner join nav.tbl_int_production_order as c
    on a."Order No_" = c."No_"

    where b."Inventory Posting Group" = 'PF' and
        a."Entry Type" = 6 /* output */ and c."Status" = 4 /* closed */ and
        a."Posting Date" >= '2021-05-01'::date
),
orders as (
    select
        b."Order No_",
        b."Item No_" as "Source No_",
        b."Item Name" as "Source Name",
        b."Range",
        b."Location Code",
        round(sum(a."Quantity"), 0) as "Output",
        max(a."Posting Date")::date as "Last Output"
    from nav.tbl_int_item_ledger_entry as a
    
    inner join pfs as b
    on a."Order No_" = b."Order No_" and a."Item No_" = b."Item No_"

    where a."Order Line No_" = 10000 and a."Entry Type" = 6 /* output */

    group by b."Order No_",
        b."Item No_",
        b."Item Name",
        b."Range",
        b."Location Code"
),
consumption as (
    select
        a."Order No_",
        b."Source No_",
        b."Source Name",
        b."Range",
        b."Output",
        b."Last Output",
        b."Location Code",
        a."Item No_",
        d."Description" as "Item Name",
        d."Inventory Posting Group",
        e."Description" as "Entry Type",
        round(-1 * sum(a."Quantity"), 2) as "Quantity",
        d."Base Unit of Measure" as "Unit of Measure",
        round(sum(c."Cost"), 2) as "Cost"
    from nav.tbl_int_item_ledger_entry as a

    inner join orders as b
    on a."Order No_" = b."Order No_"

    left join lateral (
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

    where a."Item No_" != b."Source No_" and a."Item No_" != 'SRV_EXEC'

    group by a."Order No_",
        b."Source No_",
        b."Source Name",
        b."Range",
        b."Output",
        b."Last Output",
        b."Location Code",
        a."Item No_",
        d."Description",
        d."Inventory Posting Group",
        e."Description",
        d."Base Unit of Measure"
),
ore as (
    select
        a."Order No_",
        b."Source No_",
        b."Source Name",
        b."Range",
        b."Output",
        b."Last Output",
        b."Location Code",
        a."Work Center No_",
        a."No_" as "Machine No_",
        a."Description",
        round(sum(a."Workers Qty_"), 2) as "Work Hours"
    from nav.tbl_int_capacity_ledger_entry as a

    inner join orders as b
    on b."Order No_" = a."Order No_"

    group by a."Order No_",
        b."Source No_",
        b."Source Name",
        b."Range",
        b."Output",
        b."Last Output",
        b."Location Code",
        a."Work Center No_",
        a."No_",
        a."Description"
)
select
    'ACTUAL' as "Data Version",
    'Finished' as "Status",
    a."Order No_",
    a."Source No_",
    a."Source Name",
    a."Range",
    a."Output",
    a."Last Output",
    a."Location Code",
    'MAT_CONSUMPTION' as "Value Type",
    null::text as "Work Center No_",
    a."Item No_",
    a."Item Name",
    a."Inventory Posting Group",
    a."Entry Type",
    a."Quantity" as "Quantity",
    a."Unit of Measure",
    a."Cost",
    round(a."Quantity" / a."Output", 2) as "Quantity per",
    round(a."Cost" / a."Output", 2) as "Cost per"
from consumption as a

where a."Quantity" != 0

union all

select
    'ACTUAL' as "Data Version",
    'Finished' as "Status",
    a."Order No_",
    a."Source No_",
    a."Source Name",
    a."Range",
    a."Output",
    a."Last Output",
    a."Location Code",
    'WORK_HOURS' as "Value Type",
    a."Work Center No_",
    a."Machine No_" as "Item No_",
    a."Description" as "Item Name",
    null::text as "Inventory Posting Group",
    null::text as "Entry Type",
    a."Work Hours" as "Quantity",
    'H' as "Unit of Measure",
    b."Cost",
    round(a."Work Hours" / a."Output", 2) as "Quantity per",
    round(b."Cost" / a."Output", 2) as "Cost per"
from ore as a

left join lateral (
    select round(a."Work Hours" * 9.33 * 4.95, 2) as "Cost"
) as b
on true

where a."Work Hours" != 0

union all

select
    'BDD' as "Data Version",
    null::text as "Status",
    'BDD' as "Order No_",
    a."Colet Item No_" as "Source No_",
    a."Colet Description" as "Source Name",
    a."Range",
    null::decimal as "Output",
    now()::date as "Last Output",
    a."Factory" as "Location Code",
    'MAT_CONSUMPTION' as "Value Type",
    null::text as "Work Center No_",
    a."Item No_",
    a."Item Description" as "Item Name",
    a."Item Type" as "Inventory Posting Group",
    null::text as "Entry Type",
    null::decimal as "Quantity",
    a."Unit of Measure Code" as "Unit of Measure",
    null::decimal as "Cost",
    round(a."Quantity w Scrap", 2) as "Quantity per",
    round(a."Quantity w Scrap" * a."Unit Cost", 2) as "Cost per"
from ambi.tbl_mdl_bom_comp_expanded as a

inner join (select distinct "Item No_" from pfs) as b
on a."Colet Item No_" = b."Item No_"

where a."Is Leaf" = true and a."Quantity w Scrap" != 0

union all

select
    'BDD' as "Data Version",
    null::text as "Status",
    'BDD' as "Order No_",
    a."Colet Item No_" as "Source No_",
    a."Colet Description" as "Source Name",
    b."Range",
    null::decimal as "Output",
    now()::date as "Last Output",
    a."Factory" as "Location Code",
    'WORK_HOURS' as "Value Type",
    a."Work Center No_",
    a."No_" as "Item No_",
    a."Description" as "Item Name",
    null::text as "Inventory Posting Group",
    null::text as "Entry Type",
    null::decimal as "Quantity",
    'H' as "Unit of Measure",
    null::decimal as "Cost",
    round(a."Workers Time", 2) as "Quantity per",
    round(a."Workers Time" * 9.33 * 4.95, 2) as "Cost per"
from ambi.tbl_mdl_bom_route_expanded as a

inner join (select distinct "Item No_", "Range" from pfs) as b
on a."Colet Item No_" = b."Item No_"

where a."Workers Time" != 0;