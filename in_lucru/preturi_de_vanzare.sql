/* pregatire tabela PF_KIT BOM */
drop table if exists tbl_tmp_kit_bom;

create temp table tbl_tmp_kit_bom as
    with cte as (
        select
            a."Parent Item No_",
            coalesce(nullif(b."Search Description", ''), b."Description") as "Parent Item Description",
            a."No_" as "Item No_",
            coalesce(nullif(c."Search Description", ''), c."Description") as "Item Description",
            c."Inventory Posting Group",
            a."Unit of Measure Code",
            a."Quantity per",
            e."Unit Cost",
            round(e."Unit Cost" * a."Quantity per", 3) as "Total Cost"

        from nav.tbl_int_bom_component as a

        left join nav.tbl_int_item as b
        on a."Parent Item No_" = b."No_"

        left join nav.tbl_int_item as c
        on a."No_" = c."No_"

        left join nav.tbl_int_item_umas as d
        on a."No_" = d."Item No_" and a."Unit of Measure Code" = d."Code"

        inner join lateral (select round(coalesce(c."Unit Cost", coalesce(c."Last Direct Cost", coalesce(c."Standard Cost", 0))) * coalesce(d."Qty_ per Unit of Measure", 1), 3) as "Unit Cost") as e
        on true)
    select
        *
    from cte as a
    where not exists (select * from cte as b where a."Parent Item No_" = b."Parent Item No_" and b."Unit Cost" = 0)
    order by a."Parent Item No_" asc, a."Item No_" asc;

/* calcul ponderi componente in KIT BOM */
drop table if exists tbl_tmp_kit_bom_ratios;

create temp table tbl_tmp_kit_bom_ratios as
    with cte as (
        select
            a."Parent Item No_",
            sum(a."Total Cost") as "Total Cost"
        from tbl_tmp_kit_bom as a
        group by a."Parent Item No_"
    )
    select
        a."Parent Item No_",
        a."Parent Item Description",
        a."Item No_",
        a."Item Description",
        a."Inventory Posting Group",
        a."Unit of Measure Code",
        a."Quantity per",
        a."Unit Cost",
        round(a."Total Cost"/ b."Total Cost", 5) as ratio
    from tbl_tmp_kit_bom as a
    inner join cte as b
    on a."Parent Item No_" = b."Parent Item No_";

/* Sales Type <> "Customer Price Group */
drop table if exists tbl_tmp_last_sales_price;

create temp table tbl_tmp_last_sales_price as
    select distinct on (a."Item No_", a."Sales Code", a."Currency Code")
        a."Item No_",
        coalesce(nullif(b."Search Description", ''), b."Description") as "Description",
        b."Inventory Posting Group",
        c."Description" as "Sales Type",
        a."Sales Code",
        e."Description" as "Customer Price Group",
        a."Sales Code" as "Customer Code",
        d."Name" as "Customer Name",
        d."Customer Type",
        a."Unit of Measure Code",
        a."Unit Price",
        a."Currency Code",
        a."Price Includes VAT"::int::boolean,
        a."Minimum Quantity",
        nullif(a."Starting Date", '1753-01-01'::timestamp) as "Starting Date",
        nullif(a."Ending Date", '1753-01-01'::timestamp) as "Ending Date",
        a.mod_de::date as "Sync Date"
    from nav.tbl_int_sales_price as a

    left join nav.tbl_int_item as b
    on a."Item No_" = b."No_"

    left join nav.tbl_int_sales_price_type as c
    on a."Sales Type" = c."Sales Type"

    inner join nav.tbl_int_customer as d
    on a."Sales Code" = d."No_"

    left join nav.tbl_int_customer_price_group as e
    on d."Customer Price Group" = e."Code"

    where a."Sales Type" != 1 and a."Starting Date" <= current_timestamp

    order by a."Item No_" asc, a."Sales Code" asc, a."Currency Code" asc, a."Starting Date" desc;

/* Sales Type = "Customer Price Group" */
insert into tbl_tmp_last_sales_price
select distinct on (a."Item No_", d."No_", a."Currency Code")
    a."Item No_",
    coalesce(nullif(b."Search Description", ''), b."Description") as "Description",
    b."Inventory Posting Group",
    c."Description" as "Sales Type",
    a."Sales Code",
    e."Description" as "Customer Price Group",
    d."No_" as "Customer Code",
    d."Name" as "Customer Name",
    d."Customer Type",
    a."Unit of Measure Code",
    a."Unit Price",
    a."Currency Code",
    a."Price Includes VAT"::int::boolean,
    a."Minimum Quantity",
    nullif(a."Starting Date", '1753-01-01'::timestamp) as "Starting Date",
    nullif(a."Ending Date", '1753-01-01'::timestamp) as "Ending Date",
    a.mod_de::date as "Sync Date"
from nav.tbl_int_sales_price as a

left join nav.tbl_int_item as b
on a."Item No_" = b."No_"

left join nav.tbl_int_sales_price_type as c
on a."Sales Type" = c."Sales Type"

inner join nav.tbl_int_customer as d
on a."Sales Code" = d."Customer Price Group"

left join nav.tbl_int_customer_price_group as e
on d."Customer Price Group" = e."Code"

where a."Sales Type" = 1 and a."Starting Date" <= current_timestamp and d."Customer Type" = 'CF'

order by a."Item No_" asc, d."No_" asc, a."Currency Code" asc, a."Starting Date" desc;

/* Rezultat final */
drop table if exists tbl_tmp_last_sales_price_expanded;

create temp table tbl_tmp_last_sales_price_expanded as
    select
        '1_INITIAL_RECORDS' as "Record Type",
        a."Item No_",
        a."Description",
        a."Inventory Posting Group",
        a."Sales Type",
        a."Sales Code",
        a."Customer Price Group",
        a."Customer Code",
        a."Customer Name",
        a."Customer Type",
        a."Unit of Measure Code",
        null::text as "Component No_",
        null::text as "Component Description",
        null::text as "Component Posting Group",
        null::text as "Component UM",
        null::decimal as "Component Quantity per",
        a."Unit Price" as "Price",
        a."Currency Code",
        a."Price Includes VAT",
        a."Minimum Quantity",
        a."Starting Date",
        a."Ending Date",
        a."Sync Date"
    from tbl_tmp_last_sales_price as a;

insert into tbl_tmp_last_sales_price_expanded
select
    '2_KIT_TO_COMPONENT' as "Record Type",
    a."Item No_",
    a."Description",
    a."Inventory Posting Group",
    a."Sales Type",
    a."Sales Code",
    a."Customer Price Group",
    a."Customer Code",
    a."Customer Name",
    a."Customer Type",
    a."Unit of Measure Code",
    b."Item No_" as "Component No_",
    b."Item Description" as "Component Description",
    b."Inventory Posting Group" as "Component Posting Group",
    b."Unit of Measure Code" as "Component UM",
    b."Quantity per" as "Component Quantity per",
    round(a."Unit Price" * coalesce(b.ratio, 1), 3) as "Price",
    a."Currency Code",
    a."Price Includes VAT",
    a."Minimum Quantity",
    a."Starting Date",
    a."Ending Date",
    a."Sync Date"
from tbl_tmp_last_sales_price as a

left join tbl_tmp_kit_bom_ratios as b
on a."Item No_" = b."Parent Item No_"

where a."Inventory Posting Group" = 'PF_KIT';

with cte as(
    select
        '3_COMPONENT_TO_KIT' as "Record Type",
        a."Parent Item No_" as "Item No_",
        d."Description",
        'PF_KIT' as "Inventory Posting Group",
        b."Sales Type",
        b."Sales Code",
        b."Customer Price Group",
        b."Customer Code",
        b."Customer Name",
        b."Customer Type",
        'UN' as "Unit of Measure Code",
        b."Item No_" as "Component No_",
        b."Description" as "Component Description",
        b."Inventory Posting Group" as "Component Posting Group",
        b."Unit of Measure Code" as "Component UM",
        a."Quantity per" as "Component Quantity per",
        round(b."Unit Price" * a."Quantity per", 3) as "Price",
        b."Currency Code",
        b."Price Includes VAT",
        b."Minimum Quantity",
        b."Starting Date",
        b."Ending Date",
        b."Sync Date"
    from nav.tbl_int_bom_component as a

    left join nav.tbl_int_item as d
    on a."Parent Item No_" = d."No_"

    inner join tbl_tmp_last_sales_price as b
    on a."No_" = b."Item No_" and a."Unit of Measure Code" = b."Unit of Measure Code"
    where not exists (select * from tbl_tmp_last_sales_price as c
                    where a."Parent Item No_" = c."Item No_" and b."Sales Type" = c."Sales Type" and b."Sales Code" = c."Sales Code" and
                    b."Customer Price Group" = c."Customer Price Group" and b."Customer Code" = c."Customer Code")
)
insert into tbl_tmp_last_sales_price_expanded
select
    *
from cte as a
where not exists (select * from nav.tbl_int_bom_component as b1
                    left join (select * from cte as b2
                    where b2."Sales Type" = a."Sales Type" and b2."Sales Code" = a."Sales Code" and b2."Customer Price Group" = a."Customer Price Group" and b2."Customer Code" = a."Customer Code") as b3
                    on b1."Parent Item No_" = b3."Item No_" and b1."No_" = b3."Component No_"
                where b1."Parent Item No_" = a."Item No_" and b3."Component No_" is null);
                        


select * from tbl_tmp_last_sales_price_expanded
order by "Record Type", "Sales Type", "Item No_", "Customer Code", "Sync Date" desc;