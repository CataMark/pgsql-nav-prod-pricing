select
    left(a."G_L Account No_",1) as clasa,
    a."Entry No_",
    a."Posting Date"::date,
    a."Document No_",
    a."Description",
    a."G_L Account No_",
    a."Debit Amount",
    a."Credit Amount",
    a."Amount",
    a."Global Dimension 1 Code",
    a."Global Dimension 2 Code",
    b."Description" as "Source Type",
    a."Source No_",
    coalesce(c."Name", d."Name") as "Source Name",
    a."Dimension Set ID"
from nav.tbl_int_gl_entry as a

left join nav.tbl_int_gl_entry_source_type as b
on a."Source Type" = b."Source Type"

left join nav.tbl_int_customer as c
on a."Source Type" = 1 and a."Source No_" = c."No_"

left join nav.tbl_int_vendor as d
on a."Source Type" = 2 and a."Source No_" = d."No_"

where left(a."G_L Account No_",1) in ('3', '6', '7') and
    a."Posting Date"::date between '2020-05-01'::date and '2021-04-30'::date;