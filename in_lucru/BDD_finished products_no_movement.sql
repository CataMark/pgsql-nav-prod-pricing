with cte as (
    select
        'KIT' as "Type",
        a."Kit No_",
        a."Kit Description",
        a."Kit Range" as "Range",
        a."Item No_",
        a."Item Description"
    from ambi.tbl_mdl_kit_comp_other as a
    where a."Item Type" = 'PF'

    union

    select
        'MONOCOLET' as "Type",
        a."Item No_" as "Kit No_",
        a."Description" as "Kit Description",
        a."Range",
        a."Item No_",
        a."Description" as "Item Description"
    from ambi.tbl_mdl_prod_bom_colete as a
    where not exists (select * from ambi.tbl_mdl_kit_comp_other as b where b."Item No_" = a."Item No_")
)
select
    a.*
from cte as a
where not exists (select * from nav.tbl_int_item_ledger_entry as b where b."Item No_" in (a."Kit No_", a."Item No_"));