with entries as (
    select
        y."Entry No_",
        y."User ID",
        y."Posting Date"::date,
        y."Location Code",
        m."Description" as "Item Ledger Entry Type",
        n."Description" as "Entry Type",
        y."Inventory Posting Group",
        y."Item No_",
        q."Description" as "Document Type",
        y."Description",
        y."Document No_",
        y."Valued Quantity",
        y."Cost per Unit",
        y."Sales Amount (Expected)",
        y."Sales Amount (Actual)",
        y."Cost Amount (Expected)",
        y."Cost Amount (Actual)"
    from (
        select
            b."Value Entry No_"
        from nav.tbl_int_gl_entry as a

        inner join nav.tbl_int_gl_item_ledger_relation as b
        on a."Entry No_" = b."G_L Entry No_" 

        where a."Posting Date"::date between '2022-02-01'::date and '2022-02-28'::date
    ) as x

    right join (
        select
            *
        from nav.tbl_int_value_entry as a

        where a."Posting Date"::date between '2022-02-01'::date and '2022-02-28'::date
    ) as y
    on x."Value Entry No_" = y."Entry No_"

    left join nav.tbl_int_item_entry_type as m
    on y."Item Ledger Entry Type" = m."Entry Type"

    left join nav.tbl_int_value_entry_type as n
    on y."Entry Type" = n."Entry Type"

    left join nav.tbl_int_item_entry_document_type as q
    on y."Document Type" = q."Document Type"

    where x."Value Entry No_" is null
)
select
    a.*,
    c."Posting Date"::date as "G_L Posting Date"
from entries as a

left join nav.tbl_int_gl_item_ledger_relation as b
on a."Entry No_" = b."Value Entry No_"

left join nav.tbl_int_gl_entry as c
on b."G_L Entry No_"  = c."Entry No_";