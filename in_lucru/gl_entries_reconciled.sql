select
    a."Posting Date"::date,
    a."Transaction No_",
    a."Entry No_",
    b."G_L Account No_",
    c."Name" as "G_L Account Name",
    a."Bal_ Account No_",
    b."Bal_ Account No_" as "Offset Account No_",
    d."Name" as "Offset Account Name",
    a."Document Date"::date,
    e."Description" as "Document Type",
    a."Document No_",
    a."External Document No_",
    a."Description",
    (b."Debit Amount" - b."Credit Amount") as "Amount",
    b."Debit Amount",
    b."Credit Amount",
    (b."Debit Amount (FCY)" - b."Credit Amount (FCY)") as "Amount (FCY)",
    b."Debit Amount (FCY)",
    b."Credit Amount (FCY)",
    b."Currency Code",
    a."Global Dimension 1 Code",
    a."Global Dimension 2 Code",
    a."Source Code",
    f."Description" as "Source Type",
    b."Source No_",
    coalesce(h."Name", j."Name") as "Source Name",
    a."Dimension Set ID",
    g."Description" as "FA Entry Type",
    a."FA Entry No_",
    b.tip as "Recon_ Type"
from nav.tbl_int_gl_entry as a

inner join ambi.tbl_mdl_gl_entry_reconciliation as b
on a."Transaction No_" = b."Transaction No_" and a."Entry No_" = b."Entry No_"

left join nav.tbl_int_gl_account as c
on b."G_L Account No_" = c."No_"

left join nav.tbl_int_gl_account as d
on b."Bal_ Account No_" = d."No_"

left join nav.tbl_int_gl_entry_document_type as e
on a."Document Type" = e."Document Type"

left join nav.tbl_int_gl_entry_source_type as f
on f."Source Type" = b."Source Type"

left join nav.tbl_int_gl_entry_fa_entry_type as g
on g."FA Entry Type" = a."FA Entry Type"

left join nav.tbl_int_customer as h
on b."Source Type" = 1 and b."Source No_" = h."No_"

left join nav.tbl_int_vendor as j
on b."Source Type" = 2 and b."Source No_" = j."No_"

where b."Posting Date" between '2021-05-01'::date and '2021-05-31'::date;