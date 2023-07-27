create or replace function ambi.fnc_get_unit_cost_type(
    nav_unit_cost decimal,
    nav_standard_cost decimal,
    nav_last_direct_cost decimal,
    cifraj_unit_cost decimal,
    last_purchase_price decimal,
    legacy_unit_cost decimal)
returns table (unit_cost_type text, unit_cost decimal(15, 5))
as $$
declare
    _unit_cost_type text;
    _unit_cost decimal(15, 5);
begin
    if coalesce(cifraj_unit_cost, 0) != 0 then
            _unit_cost_type := 'Cifraj';
            _unit_cost := cifraj_unit_cost;
    elsif coalesce(nav_last_direct_cost, 0) != 0 and coalesce(nav_last_direct_cost, 0) >= coalesce(nav_unit_cost,  0) and coalesce(nav_last_direct_cost, 0) >= coalesce(nav_standard_cost, 0) then
        _unit_cost_type := 'NAV Last Direct Cost';
        _unit_cost := nav_last_direct_cost;
    elsif coalesce(nav_unit_cost,  0) != 0 and coalesce(nav_unit_cost,  0) >= coalesce(nav_standard_cost, 0) and coalesce(nav_unit_cost, 0) > coalesce(nav_last_direct_cost, 0) then
        _unit_cost_type := 'NAV Unit Cost';
        _unit_cost := nav_unit_cost;
    elsif coalesce(nav_standard_cost, 0) != 0 and coalesce(nav_standard_cost, 0) > coalesce(nav_unit_cost,  0) and coalesce(nav_standard_cost, 0) > coalesce(nav_last_direct_cost, 0) then
        _unit_cost_type := 'NAV Standard Cost';
        _unit_cost := nav_standard_cost;
    elsif coalesce(last_purchase_price, 0) != 0 then 
        _unit_cost_type := 'NAV Last Purchase Price';
        _unit_cost := last_purchase_price;
    elseif coalesce(legacy_unit_cost, 0) != 0 then
        _unit_cost_type := 'Legacy Unit Cost';
        _unit_cost := legacy_unit_cost;
    else
        _unit_cost_type := 'No Unit Cost';
        _unit_cost := 0;
    end if;

    return query
        select _unit_cost_type, _unit_cost;
end;
$$ language plpgsql;


create or replace procedure ambi.prc_prod_bom_expanded()
as $$
declare
    _chrno int;
    _max_cifraj_date date;
begin
    _chrno := (select max(length(a."Line No_"::text)) from nav.tbl_int_prod_bom_line as a);

    /**********************************************/
    /* clean temporary tables */
    drop table if exists tbl_tmp_bom_vers;
    drop table if exists tbl_tmp_route_vers;
    drop table if exists tbl_tmp_items;
    drop table if exists ambi.tbl_mdl_prod_bom_colete;
    drop table if exists ambi.tbl_mdl_bom_comp_expanded;
    drop table if exists ambi.tbl_mdl_bom_route_expanded;
    drop table if exists ambi.tbl_mdl_kit_comp_other;
    drop table if exists ambi.tbl_mdl_bom_comparison;
    
    /**********************************************/
    /* prepare BOM versions */
    create temp table tbl_tmp_bom_vers on commit drop as
    select
        a."timestamp",
        a."Production BOM No_",
	    a."Version Code",
        a."Starting Date",
        a."Last Date Modified",
        (case when a."Location Code" is null or length(a."Location Code") = 0 then null::text else a."Location Code" end) as "Location Code",
        0 as priority
    from nav.tbl_int_prod_bom_vers as a
    where a."Status" = 1

    union

    select
        max(a."timestamp") as "timestamp",
        a."Production BOM No_",
        null as "Version Code",
        max(a."Starting Date") as "Starting Date",
        max(b."Last Date Modified") as "Last Date Modified",
        null as "Location Code",
        1 as priority
    from nav.tbl_int_prod_bom_line as a
    inner join nav.tbl_int_prod_bom_header as b
    on a."Production BOM No_" = b."No_"

    where b."Status" = 1 and (a."Version Code" is null or length(a."Version Code") = 0)
    group by a."Production BOM No_";

    create unique index tbl_tmp_bom_vers_ix1 on tbl_tmp_bom_vers("Production BOM No_", "Version Code", "Location Code");

    /**********************************************/
    /* prepare ROUTING versions */
    create temp table tbl_tmp_route_vers on commit drop as
    select
        a."timestamp",
        a."Routing No_",
        a."Version Code",
        a."Starting Date",
        a."Last Date Modified",
        (case when a."Location Code" is null or length(a."Location Code") = 0 then null::text else a."Location Code" end) as "Location Code",
        0 as priority
    from nav.tbl_int_routing_vers as a
    where a."Status" = 1

    union

    select
        max(a."timestamp") as "timestamp",
        a."Routing No_",
        null as "Version Code",
        '1753-01-01T00:00:00'::timestamp as "Starting Date",
        max(b."Last Date Modified") as "Last Date Modified",
        null as "Location Code",
        1 as priority
    from nav.tbl_int_routing_line as a
    inner join nav.tbl_int_routing_header as b
    on a."Routing No_" = b."No_"

    where b."Status" = 1 and (a."Version Code" is null or length(a."Version Code") = 0)
    group by a."Routing No_";

    create unique index tbl_tmp_route_vers_ix1 on tbl_tmp_route_vers("Routing No_", "Version Code", "Location Code");

    /**********************************************/
    /* prepare Items data */
    _max_cifraj_date = (select max(a.data_estim) from ambi.tbl_int_cifraj_unit_cost as a);

    create temp table tbl_tmp_items on commit drop as
    select
        a."No_",
        a."Description",
        (case when a."Range" is not null and length(a."Range") = 0 then null else a."Range" end) as "Range",
        (case when a."Inventory Posting Group" is not null and length(a."Inventory Posting Group") = 0 then null else a."Inventory Posting Group" end) as "Item Type",
        b."Manufacture Descr" as "Manufacture",
        c."Manufacture Descr" as "SAV Manufacture",
        (case when a."Production at Location" is not null and length(a."Production at Location") = 0 then null else a."Production at Location" end) as "Production at Location",
        (case when a."Production BOM No_" is not null and length(a."Production BOM No_") = 0 then null else a."Production BOM No_" end) as "Production BOM No_",
        (case when a."Routing No_" is not null and length(a."Routing No_") = 0 then null else a."Routing No_" end) as "Routing No_",
        (case when a."Technical Family" is not null and length(a."Technical Family") = 0 then null else a."Technical Family" end) as "Technical Family",
        a."Gross Weight",
        a."Unit Volume",
        (case when a."Cod ABCD" is not null and length(a."Cod ABCD") = 0 then null else a."Cod ABCD" end) as "Cod ABCD",
        a."Unit Cost" as "NAV Unit Cost",
        a."Standard Cost" as "NAV Standard Cost",
        a."Last Direct Cost" as "NAV Last Direct Cost",
        d.cost as "Cifraj Unit Cost",
        f."Last purchase price",
        e.cost as "Legacy Unit Cost",
        h.unit_cost_type as "Unit Cost Type",
        h.unit_cost as "Unit Cost",
        g."Last sales price",
        g."Price Includes VAT"
    from nav.tbl_int_item as a

    left join nav.tbl_int_item_manufacture as b
    on a."Manufacture" = b."Manufacture"

    left join  nav.tbl_int_item_manufacture as c
    on a."SAV Manufacture" = c."Manufacture"

    left join (
        select
            *
        from ambi.tbl_int_cifraj_unit_cost as d1
        where d1.data_estim = _max_cifraj_date
    ) as d
    on a."No_" = d.item_no

    left join(
        select
            *
        from ambi.tbl_int_cifraj_unit_cost as e1
        where e1.data_estim < _max_cifraj_date
    ) as e
    on a."No_" = e.item_no

    left join (
        select distinct on (f1."Item No_")
            f1."Item No_",
            (f1."Direct Unit Cost" * coalesce(f2.valoare, 1)) as "Last purchase price"
        from nav.tbl_int_purchase_price as f1

        left join ambi.tbl_int_fx_rate as f2
        on f1."Currency Code" = f2.currency

        where f1."Direct Unit Cost" != 0

        order by f1."Item No_" asc, f1."Starting Date" desc, f1."timestamp" desc
    ) as f
    on a."No_" = f."Item No_"

    left join (
        select distinct on (g1."Item No_")
            g1."Item No_",
            (g1."Unit Price" * coalesce(g2.valoare, 1)) as "Last sales price",
            (case g1."Price Includes VAT" when 0 then false else true end) as "Price Includes VAT"
        from nav.tbl_int_sales_price as g1

        left join ambi.tbl_int_fx_rate as g2
        on g1."Currency Code" = g2.currency

        where g1."Unit Price" != 0

        order by g1."Item No_" asc, g1."Starting Date" desc, g1."timestamp" desc
    ) as g
    on a."No_" = g."Item No_"

    join lateral ambi.fnc_get_unit_cost_type(a."Unit Cost", a."Standard Cost", a."Last Direct Cost", d.cost, f."Last purchase price", e.cost) as h
    on true;

    /**********************************************/
    /* prepare PF BOM headers */
    create table ambi.tbl_mdl_prod_bom_colete as
    select
        b."Item No_",
        b."Description",
        b."Range",
        b."Item Type",
        b."Cod ABCD",
        b."Gross Weight",
        b."Unit Volume",
        b."Last sales price",
        b."Price Includes VAT",
        b."NAV Unit Cost",
        b."NAV Standard Cost",
        b."NAV Last Direct Cost",
        a."No_" as "BOM No_",
        c."BOM Version",
        c."BOM Location",
        b."Factory",
        b."Routing No_",
        e."Route Version",
        e."Route Location",
        (case when coalesce(d."Avg Quantity", 0) < 50 then 'Default' else 'Calculated' end) as "Avg Qty Type",
        (case when coalesce(d."Avg Quantity", 0) < 50 then 200 else d."Avg Quantity" end) as "Avg Quantity"
    from nav.tbl_int_prod_bom_header as a
    
    inner join(
        select distinct on (b1."Production BOM No_")
            b1."No_" as "Item No_",
            b1."Description",
            b1."Item Type",
            coalesce(coalesce(b1."Manufacture", b1."SAV Manufacture"), b1."Production at Location") as "Factory",
            b1."Production BOM No_",
            b1."Routing No_",
            b1."Cod ABCD",
            b1."Range",
            b1."Gross Weight",
            b1."Unit Volume",
            b1."NAV Unit Cost",
            b1."NAV Standard Cost",
            b1."NAV Last Direct Cost",
            b1."Last sales price",
            b1."Price Includes VAT"
        from tbl_tmp_items as b1

        order by b1."Production BOM No_" asc, (case when b1."No_" = b1."Production BOM No_" then 1 else 0 end) desc
    ) as b
    on a."No_" = b."Production BOM No_"

    left join lateral (
        select
            c1."Version Code" as "BOM Version",
            c1."Location Code" as "BOM Location"
        from tbl_tmp_bom_vers as c1
        where c1."Production BOM No_" = a."No_" and c1."Starting Date" <= current_timestamp
        order by (case when b."Factory" is not null and c1."Location Code" = b."Factory" then 2 else c1.priority end) desc,
                c1."Starting Date" desc nulls last, c1."Last Date Modified" desc nulls last, c1."timestamp" desc
        limit 1
    ) as c
    on true

    left join (
        select
            d1."Source No_",
            round(avg(d1."Quantity") / 100) * 100 as "Avg Quantity"
        from nav.tbl_int_production_order as d1
        where d1."Status" != 0::int and d1."Quantity" > 50::decimal(38, 20)
        group by d1."Source No_"
    ) as d
    on a."No_" = d."Source No_"

    left join lateral (
        select
            e1."Version Code" as "Route Version",
            e1."Location Code" as "Route Location"
        from tbl_tmp_route_vers as e1
        where e1."Routing No_" = b."Routing No_" and e1."Starting Date" <= current_timestamp
        order by (case when (b."Factory" is not null or c."BOM Location" is not null) and e1."Location Code" = coalesce(c."BOM Location", b."Factory") then 2
                    else e1.priority end) desc,
                e1."Starting Date" desc nulls last, e1."Last Date Modified" desc nulls last, e1."timestamp" desc
        limit 1
    ) as e
    on true

    inner join (
        select distinct
            f1."Production BOM No_"
        from tbl_tmp_bom_vers as f1
    ) as f
    on a."No_" = f."Production BOM No_"

    left join (
        select distinct
            g1."No_" as "Item No_"
        from nav.tbl_int_bom_component as g1

        inner join tbl_tmp_items as g2
        on g1."Parent Item No_" = g2."No_"

        where g2."Cod ABCD" != 'D' --produs anulat
    ) as g
    on b."Item No_" = g."Item No_"

    where b."Item Type" like 'PF%' and (g."Item No_" is not null or b."Cod ABCD" != 'D'); --produs anulat fara kit activ

    /**********************************************/
    /* prepare BOM components expanded */
    create table ambi.tbl_mdl_bom_comp_expanded as
    with recursive bom as(
        select
            a."Item No_" as "Colet Item No_",
            a."Description" as "Colet Description",
            a."Range",
            a."Avg Qty Type" as "Colet Avg Qty Type",
            a."Avg Quantity" as "Colet Avg Qty",
            0::int as "Level",
            (repeat('0', _chrno - length('10000')) || '10000_') as "Line No_",
            1::int as "Line Type",
            false as "Is Leaf",
            a."Item No_",
            a."Description" as "Item Description",
            a."Factory",
            a."BOM No_",
            a."BOM Version",
            a."BOM Location",
            a."Routing No_",
            a."Route Version",
            a."Route Location",
            null::text as "Routing Link Code",
            a."Item Type",
            null::text as "Item Family",
            1::decimal(38, 20) as "Quantity per",
            1::decimal(38, 20) as "Quantity",
            0::decimal(38, 20) as "Scrap _",
            null::decimal(38, 20) as "Parent Route Fixed Scrap Qty",
            null::decimal(38, 20) as "Parent Route Scrap Factor",
            null::decimal(38, 20) as "Parent Route Fixed Scrap Qty Accum",
            null::decimal(38, 20) as "Parent Route Scrap Factor Accum",
            'UN'::text as "Unit of Measure Code",
            1::decimal(38, 20) as "Quantity w Scrap",
            null::decimal(15,5) as "NAV Unit Cost",
            null::decimal(15,5) as "NAV Standard Cost",
            null::decimal(15,5) as "NAV Last Direct Cost",
            null::decimal(15,5) as "Cifraj Unit Cost",
            null::decimal(15,5) as "Last purchase price",
            null::decimal(15,5) as "Legacy Unit Cost",
            null::text as "Unit Cost Type",
            null::decimal(15,5) as "Unit Cost"
        from ambi.tbl_mdl_prod_bom_colete as a

        union all

        select
            f."Colet Item No_",
            f."Colet Description",
            f."Range",
            f."Colet Avg Qty Type",
            f."Colet Avg Qty",
            f."Level" + 1 as "Level",
            (f."Line No_" || a."Line No_" || '_') as "Line No_",
            a."Type" as "Line Type",
            (case when a."Type" != 2 and (c."Item Type" = 'MAT_PRIMA' or c."Production BOM No_" is null) then true else false end) as "Is Leaf",
            a."No_" as "Item No_",
            coalesce(c."Description", a."Description") as "Item Description",
            f."Factory",
            (case when c."Item Type" = 'MAT_PRIMA' then null else (case when a."Type" = 2 then a."No_" else c."Production BOM No_" end) end) as "BOM No_",
            d."Item BOM Version" as "BOM Version",
            d."Item BOM Location" as "BOM Location",
            c."Routing No_" as "Routing No_",
            e."Item Route Version" as "Route Version",
            e."Item Route Location" as "Route Location",
            a."Routing Link Code",
            c."Item Type",
            c."Technical Family" as "Item Family",            
            a."Quantity per",
            (f."Quantity" * a."Quantity per")::decimal(38, 20) as "Quantity",
            a."Scrap _",
            g."Fixed Scrap Quantity" as "Parent Route Fixed Scrap Qty",
            g."Scrap Factor _" as "Parent Route Scrap Factor",
            g."Fixed Scrap Qty_ (Accum_)" as "Parent Route Fixed Scrap Qty Accum",
            g."Scrap Factor _ (Accumulated)" as "Parent Route Scrap Factor Accum",
            a."Unit of Measure Code",
            ((f."Quantity w Scrap" * a."Quantity per" * (1 + a."Scrap _" / 100) * (1 + coalesce(g."Scrap Factor _", 0) / 100) * (1 + coalesce(g."Scrap Factor _ (Accumulated)", 0) / 100))
                                        + coalesce(g."Fixed Scrap Qty_ (Accum_)", 0))::decimal(38, 20) as "Quantity w Scrap",
            c."NAV Unit Cost"::decimal(15,5),
            c."NAV Standard Cost"::decimal(15,5),
            c."NAV Last Direct Cost"::decimal(15,5),
            c."Cifraj Unit Cost"::decimal(15,5),
            c."Last purchase price"::decimal(15,5),
            c."Legacy Unit Cost"::decimal(15,5),
            c."Unit Cost Type",
            c."Unit Cost"::decimal(15,5)
        from nav.tbl_int_prod_bom_line as a

        inner join tbl_tmp_bom_vers as b
        on a."Production BOM No_" = b."Production BOM No_" and
            (case when a."Version Code" is null or length(a."Version Code") = 0 then '_null' else a."Version Code" end) = coalesce(b."Version Code", '_null')

        inner join bom as f
        on f."Is Leaf" = false and b."Production BOM No_" = f."BOM No_" and coalesce(b."Version Code", '_null') = coalesce(f."BOM Version", '_null')

        left join tbl_tmp_items as c
        on a."No_" = c."No_"

        left join lateral(
            select
                d1."Version Code" as "Item BOM Version",
                d1."Location Code" as "Item BOM Location"
            from tbl_tmp_bom_vers as d1
            where (case when c."Item Type" = 'MAT_PRIMA' then null else (case when a."Type" = 2 then a."No_" else c."Production BOM No_" end) end) = d1."Production BOM No_" and
                d1."Starting Date" <= current_timestamp
            order by (case when d1."Location Code" = coalesce(b."Location Code", f."Factory") then 2 else d1.priority end) desc,
                    d1."Starting Date" desc nulls last, d1."Last Date Modified" desc nulls last, d1."timestamp" desc
            limit 1
        ) as d
        on true

        left join lateral(
            select
                e1."Version Code" as "Item Route Version",
                e1."Location Code" as "Item Route Location"                
            from tbl_tmp_route_vers as e1

            where e1."Routing No_" = c."Routing No_" and e1."Starting Date" <= current_timestamp
            order by (case when e1."Location Code" = coalesce(d."Item BOM Location", f."Factory") then 2 else e1.priority end) desc,
                    e1."Starting Date" desc nulls last, e1."Last Date Modified" desc nulls last
            limit 1
        ) as e
        on true

        left join lateral(
            select
                g."Fixed Scrap Quantity",
                g."Scrap Factor _",
                g."Fixed Scrap Qty_ (Accum_)",
                g."Scrap Factor _ (Accumulated)"
            from nav.tbl_int_routing_line as g
            where g."Routing No_" = f."Routing No_" and
                (case when g."Version Code" is null or length(g."Version Code") = 0 then '_null' else g."Version Code" end) = coalesce(f."Route Version", '_null') and
                a."Routing Link Code" = g."Routing Link Code"
            order by g."Operation No_" asc
            limit 1
        ) as g
        on true
    )
    select
        *
    from bom as a;

    update ambi.tbl_mdl_bom_comp_expanded set
        "NAV Unit Cost" = null,
        "NAV Standard Cost" = null,
        "NAV Last Direct Cost" = null,
        "Cifraj Unit Cost" = null,
        "Unit Cost Type" = null,
        "Unit Cost" = null
    where "Is Leaf" = false;

    create index tbl_mdl_bom_comp_expanded_ix1 on ambi.tbl_mdl_bom_comp_expanded ("Colet Item No_", "Item No_");

    /**********************************************/
    /* prepare BOM routings expanded */
    create table ambi.tbl_mdl_bom_route_expanded as
    select
        b."Colet Item No_",
        b."Colet Description",
        b."Colet Avg Qty Type",
        b."Colet Avg Qty",
        b."Level",
        b."Line No_",
        b."Item No_",
        b."Item Description",
        b."Factory",
        b."BOM No_",
        b."BOM Version",
        b."BOM Location",
        b."Quantity w Scrap",
        b."Unit of Measure Code",
        b."Routing No_",
        b."Route Version",
        b."Route Location",
        a."Operation No_",
        a."Next Operation No_",
        a."Previous Operation No_",
        a."Type",
        a."No_",
        a."Description",
        a."Work Center No_",
        a."Work Center Group Code",
        a."Setup Time",
        a."Setup Time Unit of Meas_ Code",
        a."Run Time",
        a."Run Time Unit of Meas_ Code",
        a."Routing Link Code",
        a."Machine Efficiency (_)",
        a."No_ Of Workers",
        round(a."Setup Time" / b."Colet Avg Qty" + a."Run Time" * b."Quantity w Scrap", 5) as "Machine Time",
        round((a."Setup Time" / b."Colet Avg Qty" + a."Run Time" * b."Quantity w Scrap") * a."No_ Of Workers", 5) as "Workers Time"
    from nav.tbl_int_routing_line as a

    inner join ambi.tbl_mdl_bom_comp_expanded as b
    on a."Routing No_" = b."Routing No_" and
        (case when a."Version Code" is null or length(a."Version Code") = 0 then '_null' else a."Version Code" end) = coalesce(b."Route Version", '_null');

    create index tbl_mdl_bom_route_expanded_ix1 on ambi.tbl_mdl_bom_route_expanded ("Colet Item No_", "Item No_");

    /**********************************************/
    /* prepare other kit components */
    create table ambi.tbl_mdl_kit_comp_other as
    select
        a."Parent Item No_" as "Kit No_",
        b."Description" as "Kit Description",
        b."Range" as "Kit Range",
        b."Cod ABCD",
        a."No_" as "Item No_",
        a."Description" as "Item Description",
        c."Item Type",
        coalesce(coalesce(coalesce(b."Manufacture", b."SAV Manufacture"), b."Production at Location"), d."Factory") as "Factory",
        c."Technical Family" as "Item Family",
        a."Quantity per",
        a."Unit of Measure Code",
        c."Gross Weight",
        c."Unit Volume",
        (case when d."Item No_" is null then c."Unit Cost Type" else 'BOM' end) as "Unit Cost Type",
        (case when d."Item No_" is null then c."Unit Cost" else null end) as "Unit Cost"
    from nav.tbl_int_bom_component as a

    inner join tbl_tmp_items as b
    on a."Parent Item No_" = b."No_"

    left join tbl_tmp_items as c
    on a."No_" = c."No_"

    left join ambi.tbl_mdl_prod_bom_colete as d
    on c."No_" = d."Item No_"

    where b."Cod ABCD" != 'D';
    /**********************************************/
    /* prepare bom comparison */
    create table ambi.tbl_mdl_bom_comparison as
    with orders as (
        select
            b."Order No_",
            b."Order Status",
            b."Order BOM No_",
            b."Order BOM Version",
            a."BOM No_" as "BDD BOM No_",
            a."Description" as "BDD BOM Description",
            a."BOM Version" as "BDD BOM Version",
            b."Quantity" as "Order Qty"
        from ambi.tbl_mdl_prod_bom_colete as a

        left join lateral(
            select
                b1."Prod_ Order No_" as "Order No_",
                b2."Description" as "Order Status",
                b1."Production BOM No_" as "Order BOM No_",
                b1."Production BOM Version Code" as "Order BOM Version",
                b1."Quantity"
            from nav.tbl_int_prod_order_line as b1

            left join nav.tbl_int_production_order_status as b2
            on b1."Status" = b2."Status"

            where b1."Production BOM No_" = a."BOM No_" and b1."Prod_ Order No_" like 'PO%' and b1."Line No_" = 10000 and b1."Status" >= 2 and
                not exists (select * from nav.tbl_int_prod_order_component as b3
                            where b3."Prod_ Order No_" = b1."Prod_ Order No_" and (b3."Quantity per" is null or b3."Quantity per" = 0::decimal(38, 20)))
            order by b1."Status" asc, (case when (a."BOM Version" is null and (b1."Production BOM Version Code" is null or length(b1."Production BOM Version Code") = 0)) or
                                                    a."BOM Version" = b1."Production BOM Version Code" then 1
                                        else 0 end) desc
            limit 1
        ) as b
        on true

        where b."Order No_" is not null
    )
    select
        r."Order No_",
        r."Order Status",
        r."Order BOM No_",
        r."Order BOM Version",
        r."Order Qty",
        r."BDD BOM No_",
        r."BDD BOM Description",
        r."BDD BOM Version",
        r."Source",
        r."Type",
        r."Item No_",
        s."Description" as "Description",
        s."Inventory Posting Group" as "Item Type",
        r."UMAS",
        round(r."Expect Qty", 5) as "Expect Qty",
        (case when r."Type" = 'Component' and (s."Inventory Posting Group" = 'MAT_PRIMA' or s."Production BOM No_" is null or length(s."Production BOM No_") = 0)
                then coalesce(t.cost, greatest(s."Unit Cost", s."Standard Cost", s."Last Direct Cost")) else null end) as "Unit Cost"
    from (
        select
            b."Order No_",
            b."Order Status",
            b."Order BOM No_",
            b."Order BOM Version",
            b."Order Qty",
            b."BDD BOM No_",
            b."BDD BOM Description",
            b."BDD BOM Version",
            'NAV' as "Source",
            'Component' as "Type",
            a."Item No_",
            a."Unit of Measure Code" as "UMAS",
            sum(a."Expected Quantity") as "Expect Qty"
        from nav.tbl_int_prod_order_component as a
        
        inner join orders as b
        on a."Prod_ Order No_" = b."Order No_"

        group by
            b."Order No_",
            b."Order Status",
            b."Order BOM No_",
            b."BDD BOM Description",
            b."Order BOM Version",
            b."Order Qty",
            b."BDD BOM No_",
            b."BDD BOM Description",
            b."BDD BOM Version",
            a."Item No_",
            a."Unit of Measure Code"

        union all
        
        select
            b."Order No_",
            b."Order Status",
            b."Order BOM No_",
            b."Order BOM Version",
            b."Order Qty",
            b."BDD BOM No_",
            b."BDD BOM Description",
            b."BDD BOM Version",
            'BDD' as "Source",
            'Component' as "Type",
            a."Item No_",
            a."Unit of Measure Code" as "UMAS",
            sum(a."Quantity w Scrap" * b."Order Qty") as "Expect Qty"
        from ambi.tbl_mdl_bom_comp_expanded as a

        inner join orders as b
        on b."BDD BOM No_"  = a."Colet Item No_"

        where a."Level" != 0

        group by
            b."Order No_",
            b."Order Status",
            b."Order BOM No_",
            b."Order BOM Version",
            b."Order Qty",
            b."BDD BOM No_",
            b."BDD BOM Description",
            b."BDD BOM Version",
            a."Item No_",
            a."Unit of Measure Code"

        union all

        select
            b."Order No_",
            b."Order Status",
            b."Order BOM No_",
            b."Order BOM Version",
            b."Order Qty",
            b."BDD BOM No_",
            b."BDD BOM Description",
            b."BDD BOM Version",
            'NAV' as "Source",
            'Routing' as "Type",
            c."Item No_",
            'ORE' as "UMAS",
            sum(a."Estim Workers Time") as "Expect Qty"
        from nav.tbl_int_prod_order_routing_line as a

        inner join orders as b
        on a."Prod_ Order No_" = b."Order No_"

        left join nav.tbl_int_prod_order_line as c
        on a."Prod_ Order No_" = c."Prod_ Order No_" and a."Routing Reference No_" = c."Line No_" and a."Routing No_" = c."Routing No_"

        group by
            b."Order No_",
            b."Order Status",
            b."Order BOM No_",
            b."Order BOM Version",
            b."Order Qty",
            b."BDD BOM No_",
            b."BDD BOM Description",
            b."BDD BOM Version",
            c."Item No_"
            
        union all
        
        select
            b."Order No_",
            b."Order Status",
            b."Order BOM No_",
            b."Order BOM Version",
            b."Order Qty",
            b."BDD BOM No_",
            b."BDD BOM Description",
            b."BDD BOM Version",
            'BDD' as "Source",
            'Routing' as "Type",
            a."Item No_",
            'ORE' as "UMAS",
            sum((a."Setup Time" + a."Run Time" * a."Quantity w Scrap" * b."Order Qty") * a."No_ Of Workers") as "Expect Qty"
        from ambi.tbl_mdl_bom_route_expanded as a

        inner join orders as b
        on b."BDD BOM No_"  = a."Colet Item No_"

        group by
            b."Order No_",
            b."Order Status",
            b."Order BOM No_",
            b."Order BOM Version",
            b."Order Qty",
            b."BDD BOM No_",
            b."BDD BOM Description",
            b."BDD BOM Version",
            a."Item No_"
    ) as r

    left join nav.tbl_int_item as s
    on r."Item No_" = s."No_"

    left join ambi.tbl_int_cifraj_unit_cost as t
    on r."Item No_" = t.item_no;
end;
$$ language plpgsql;