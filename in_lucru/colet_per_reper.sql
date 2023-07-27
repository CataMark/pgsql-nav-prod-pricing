select
    y."Item No_",
    x."No_" as colet,
    x."Description" as colet_description,
    x."Range"
from nav.tbl_int_item as x

inner join (
    select distinct
        a."Item No_",
        first_value(b."Source No_") over (partition by a."Item No_" order by b."Location Code" desc, b."Status" asc
                                        rows between unbounded preceding and unbounded following) as colet
    from nav.tbl_int_prod_order_component as a

    inner join nav.tbl_int_production_order as b
    on a."Prod_ Order No_" = b."No_"

    where b."Status" >= 3) as y
on x."Production BOM No_" = y."colet";