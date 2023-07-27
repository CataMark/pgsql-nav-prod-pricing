with purchased as (
    select distinct
        a."Item No_"
    from nav.tbl_int_value_entry as a
    where a."Item Ledger Entry Type" = 0 /* purchase */ and
        a."Inventory Posting Group" in ('SEMIFABR', 'PF', 'PF_KIT', 'SERV_EXEC', 'WIP', 'PROD EXE', 'PROD REZ', 'PROTOTIP')
)
select
    date_part('year', a."Posting Date")::int as "Year",
    date_part('month', a."Posting Date")::int as "Month",
    a."Location Code",
    a."Item No_",
    b."Description" as "Item Name",
    a."Inventory Posting Group",
    c."Description" as "Technical Family",
    b."FSC",
    b."Base Unit of Measure",
    b."Unit Cost",
    sum(a."Item Ledger Entry Quantity") as "Item Ledger Entry Quantity",
    sum(a."Cost Amount (Actual)") as "Cost Amount (Actual)",
    sum(a."Cost Amount (Expected)") as "Cost Amount (Expected)"
from nav.tbl_int_value_entry as a

left join nav.tbl_int_item as b
on a."Item No_" = b."No_"

left join nav.tbl_int_technical_family_ as c
on b."Technical Family" = c."Code"

left join purchased as d
on a."Item No_" = d."Item No_"

where a."Item Ledger Entry Type" = 0 /* consumption */
    and a."Posting Date"::date between '2021-05-01'::date and '2021-12-31'::date and
    (case when a."Inventory Posting Group" not in ('SEMIFABR', 'PF', 'PF_KIT', 'SERV_EXEC', 'WIP', 'PROD EXE', 'PROD REZ', 'PROTOTIP') then true
        else d."Item No_" is not null end) = true
    
group by "Year", "Month", a."Location Code", a."Item No_", b."Description", a."Inventory Posting Group", c."Description", b."FSC", b."Base Unit of Measure", b."Unit Cost";