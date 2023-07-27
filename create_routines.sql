do $$

declare

    _dbname text := current_database();

begin

    if _dbname != 'any' then

        raise exception 'not in "any" database';

    end if;

end;

$$ language plpgsql;

/* 0001.01 */

do $$

begin

    raise notice '************************************';

    raise notice 'creating procedure "prc_cifraj_unit_cost_merge_load"';

end;

$$ language plpgsql;



create or replace procedure ambi.prc_cifraj_unit_cost_merge_load()

as $$

begin

    insert into ambi.tbl_int_cifraj_unit_cost as t (item_no, descr, umas, cost, data_estim, mod_de)

    select item_no, descr, umas, cost, data_estim, mod_de

    from ambi.tbl_int_cifraj_unit_cost_load

    on conflict on constraint tbl_int_cifraj_unit_cost_pk

    do update set

        descr = excluded.descr,

        umas = excluded.umas,

        cost = excluded.cost,

        data_estim = excluded.data_estim,

        mod_de = excluded.mod_de

    where excluded.mod_de >= t.mod_de;



    delete from ambi.tbl_int_cifraj_unit_cost_load;

end;

$$ language plpgsql;

/* 0001.02 */

do $$

begin

    raise notice '************************************';

    raise notice 'creating procedure "prc_prod_bom_expanded"';

end;

$$ language plpgsql;



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



/********************************************************/

/********************************************************/

create or replace function ambi.fnc_umas_covert(

    item_no text,

    source_umas text,

    target_umas text

) returns decimal(38, 20)

as $$

declare

    _source_qty decimal(38, 20);

    _target_qty decimal(38, 20);

    _rezultat decimal(38, 20);

begin

    _source_qty := coalesce((select "Qty_ per Unit of Measure" from nav.tbl_int_item_umas where upper("Item No_") = upper(item_no) and upper("Code") = upper(source_umas)), 0);

    _target_qty := coalesce((select "Qty_ per Unit of Measure" from nav.tbl_int_item_umas where upper("Item No_") = upper(item_no) and upper("Code") = upper(target_umas)), 0);

    if _source_qty = 0 or _target_qty = 0 then

        _rezultat := -1;

    else

        _rezultat := _target_qty/ _source_qty;

    end if;



    return _rezultat;

end;

$$ language plpgsql;



/********************************************************/

/********************************************************/

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



    /**********************************************/

    /* prepare BOM versions */

    create temp table tbl_tmp_bom_vers on commit drop as

    select

        a."timestamp",

        a."Production BOM No_",

	    a."Version Code",

        a."Starting Date"::date,

        a."Last Date Modified",

        nullif(a."Location Code", '') as "Location Code",

        0 as priority

    from nav.tbl_int_prod_bom_vers as a

    where a."Status" in (0, 1) -- "new" + "certified"



    union



    select

        max(a."timestamp") as "timestamp",

        a."Production BOM No_",

        null as "Version Code",

        max(a."Starting Date"::date) as "Starting Date",

        max(b."Last Date Modified") as "Last Date Modified",

        null as "Location Code",

        1 as priority

    from nav.tbl_int_prod_bom_line as a

    inner join nav.tbl_int_prod_bom_header as b

    on a."Production BOM No_" = b."No_"



    where b."Status" in (0, 1) and (a."Version Code" is null or length(a."Version Code") = 0)

    group by a."Production BOM No_";



    create unique index tbl_tmp_bom_vers_ix1 on tbl_tmp_bom_vers("Production BOM No_", "Version Code", "Location Code");



    /**********************************************/

    /* prepare ROUTING versions */

    create temp table tbl_tmp_route_vers on commit drop as

    select

        a."timestamp",

        a."Routing No_",

        a."Version Code",

        a."Starting Date"::date,

        a."Last Date Modified",

        nullif(a."Location Code", '') as "Location Code",

        0 as priority

    from nav.tbl_int_routing_vers as a

    where a."Status" in (0, 1) -- "new" + "certified"



    union



    select

        max(a."timestamp") as "timestamp",

        a."Routing No_",

        null as "Version Code",

        '1753-01-01'::date as "Starting Date",

        max(b."Last Date Modified") as "Last Date Modified",

        null as "Location Code",

        1 as priority

    from nav.tbl_int_routing_line as a

    inner join nav.tbl_int_routing_header as b

    on a."Routing No_" = b."No_"



    where b."Status" in (0, 1) and (a."Version Code" is null or length(a."Version Code") = 0)

    group by a."Routing No_";



    create unique index tbl_tmp_route_vers_ix1 on tbl_tmp_route_vers("Routing No_", "Version Code", "Location Code");



    /**********************************************/

    /* prepare Items data */

    _max_cifraj_date := (select max(a.data_estim) from ambi.tbl_int_cifraj_unit_cost as a);



    create temp table tbl_tmp_items on commit drop as

    select

        a."No_",

        a."Description",

        nullif(a."Range", '') as "Range",

        nullif(a."Inventory Posting Group", '') as "Item Type",

        coalesce(coalesce(b."Manufacture Descr", c."Manufacture Descr"), nullif(a."Production at Location", '')) as "Factory",

        nullif(a."Production BOM No_", '') as "Production BOM No_",

        nullif(a."Routing No_", '') as "Routing No_",

        nullif(a."Technical Family", '') as "Technical Family",

        a."Gross Weight",

        a."Net Weight",

        nullif(j."Length" / 1000, 0) as "Length",

        nullif(j."Width" / 1000, 0) as "Width",

        nullif(j."Height" / 1000, 0) as "Height",

        a."Unit Volume",

        nullif(a."Cod ABCD", '') as "Cod ABCD",

        a."Base Unit of Measure" as "Unit of Measure",

        a."Unit Cost" as "NAV Unit Cost",

        a."Standard Cost" as "NAV Standard Cost",

        a."Last Direct Cost" as "NAV Last Direct Cost",

        d.cost * abs(d.cnv_rate) as "Cifraj Unit Cost",

        f."Last purchase price" * abs(f.cnv_rate) as "Last purchase price",

        e.cost * abs(e.cnv_rate) as "Legacy Unit Cost",

        h.unit_cost_type as "Unit Cost Type",

        h.unit_cost as "Unit Cost",

        g."Last sales price" * abs(g.cnv_rate) as "Last sales price",

        g."Price Includes VAT",

        (case d.cnv_rate when -1 then 'CIFRAJ;' else '' end) ||

        (case e.cnv_rate when -1 then 'LECACY;' else '' end) ||

        (case f.cnv_rate when -1 then 'PURCHASE;' else '' end) ||

        (case g.cnv_rate when -1 then 'SALES;' else '' end) as "UMAS Convert Errors"

    from nav.tbl_int_item as a



    left join nav.tbl_int_item_manufacture as b

    on a."Manufacture" = b."Manufacture"



    left join  nav.tbl_int_item_manufacture as c

    on a."SAV Manufacture" = c."Manufacture"



    left join lateral(

        select

            d1.cost,

            (case when nullif(d1.umas, '') is null or upper(d1.umas) = upper(a."Base Unit of Measure") then 1 else ambi.fnc_umas_covert(d1.item_no, d1.umas, a."Base Unit of Measure") end) as cnv_rate

        from ambi.tbl_int_cifraj_unit_cost as d1



        where a."No_" = d1.item_no and d1.data_estim = _max_cifraj_date

    ) as d

    on true



    left join lateral(

        select

            e1.cost,

            (case when nullif(e1.umas, '') is null or upper(e1.umas) = upper(a."Base Unit of Measure") then 1 else ambi.fnc_umas_covert(e1.item_no, e1.umas, a."Base Unit of Measure") end) as cnv_rate

        from ambi.tbl_int_cifraj_unit_cost as e1



        where a."No_" = e1.item_no and e1.data_estim < _max_cifraj_date

    ) as e

    on true



    left join lateral (

        select distinct on (f1."Item No_")

            f1."Item No_",

            (f1."Direct Unit Cost" * coalesce(f2.valoare, 1)) as "Last purchase price",

            (case when coalesce(nullif(f1."Unit of Measure Code", ''), nullif(a."Purch_ Unit of Measure", '')) is null or

                        upper(coalesce(nullif(f1."Unit of Measure Code", ''), a."Purch_ Unit of Measure")) = upper(a."Base Unit of Measure") then 1

                else ambi.fnc_umas_covert(f1."Item No_", coalesce(nullif(f1."Unit of Measure Code", ''), nullif(a."Purch_ Unit of Measure", '')), a."Base Unit of Measure") end) as cnv_rate

        from nav.tbl_int_purchase_price as f1



        left join ambi.tbl_int_fx_rate as f2

        on f1."Currency Code" = f2.currency



        where a."No_" = f1."Item No_" and f1."Direct Unit Cost" != 0



        order by f1."Item No_" asc, f1."Starting Date" desc, f1."timestamp" desc

    ) as f

    on true



    left join lateral (

        select distinct on (g1."Item No_")

            g1."Item No_",

            (g1."Unit Price" * coalesce(g2.valoare, 1)) as "Last sales price",

            (case when coalesce(nullif(g1."Unit of Measure Code", ''), nullif(a."Sales Unit of Measure", '')) is null or

                        upper(coalesce(nullif(g1."Unit of Measure Code", ''), a."Sales Unit of Measure", '')) = upper(a."Base Unit of Measure") then 1

                else ambi.fnc_umas_covert(g1."Item No_", coalesce(nullif(g1."Unit of Measure Code", ''), nullif(a."Sales Unit of Measure", '')), a."Base Unit of Measure") end) as cnv_rate,

            (case g1."Price Includes VAT" when 0 then false else true end) as "Price Includes VAT"

        from nav.tbl_int_sales_price as g1



        left join ambi.tbl_int_fx_rate as g2

        on g1."Currency Code" = g2.currency



        where a."No_" = g1."Item No_" and g1."Unit Price" != 0



        order by g1."Item No_" asc, g1."Starting Date" desc, g1."timestamp" desc

    ) as g

    on true



    join lateral ambi.fnc_get_unit_cost_type(a."Unit Cost", a."Standard Cost", a."Last Direct Cost", d.cost * abs(d.cnv_rate), f."Last purchase price" * abs(f.cnv_rate), e.cost * abs(e.cnv_rate)) as h

    on true



    left join nav.tbl_int_item_umas as j

    on a."No_" = j."Item No_" and a."Base Unit of Measure" = j."Code";



    create unique index tbl_tmp_items_ix1 on tbl_tmp_items("No_");



    /**********************************************/

    /* prepare PF BOM headers */

    create table ambi.tbl_mdl_prod_bom_colete as

    select

        b."No_" as "Item No_",

        b."Description",

        b."Range",

        b."Item Type",

        b."Cod ABCD",

        b."Gross Weight",

        b."Net Weight",

        b."Length",

        b."Width",

        b."Height",

        b."Unit Volume",

        b."Unit of Measure",

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

        (case when coalesce(d."Avg Quantity", 0) < 50 then 200 else d."Avg Quantity" end) as "Avg Quantity",

        g."First Prod_ Date",

        h."First Sale Date"

    from nav.tbl_int_prod_bom_header as a

    

    inner join tbl_tmp_items as b

    on a."No_" = b."Production BOM No_"



    left join lateral (

        select

            c1."Version Code" as "BOM Version",

            c1."Location Code" as "BOM Location"

        from tbl_tmp_bom_vers as c1

        where c1."Production BOM No_" = a."No_" and c1."Starting Date" <= current_date

        order by (case when c1."Location Code" = b."Factory" then 2 else c1.priority end) desc, c1."Starting Date" desc nulls last, c1."Last Date Modified" desc nulls last, c1."timestamp" desc

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

        where e1."Routing No_" = b."Routing No_" and e1."Starting Date" <= current_date

        order by (case when e1."Location Code" = coalesce(c."BOM Location", b."Factory") then 2 else e1.priority end) desc, e1."Starting Date" desc nulls last, e1."Last Date Modified" desc nulls last,

                e1."timestamp" desc

        limit 1

    ) as e

    on true



    inner join (

        select distinct

            f1."Production BOM No_"

        from tbl_tmp_bom_vers as f1

    ) as f

    on a."No_" = f."Production BOM No_"



    left join lateral(

        select

            min(g1."Posting Date"::date) as "First Prod_ Date"

        from nav.tbl_int_item_ledger_entry as g1



        where g1."Item No_" = b."No_" and g1."Entry Type" = 6 /* output */



        group by g1."Item No_"

    ) as g

    on true



    left join lateral(

        select

            min(h1."Posting Date"::date) as "First Sale Date"

        from nav.tbl_int_item_ledger_entry as h1



        where h1."Item No_" = b."No_" and h1."Entry Type" = 1 /* sale */



        group by h1."Item No_"

    ) as h

    on true



    where b."Item Type" like 'PF%'; /* and (g."Item No_" is not null or b."Cod ABCD" != 'D' or b.) --produs anulat fara kit activ */



    create unique index tbl_mdl_prod_bom_colete_ix1 on ambi.tbl_mdl_prod_bom_colete("Item No_");



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

            null::decimal(15,5) as "Unit Cost",

            null::text as "UMAS Convert Errors"

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

            (c."NAV Unit Cost" * abs(c.cnv_rate))::decimal(15,5) as "NAV Unit Cost",

            (c."NAV Standard Cost" * abs(c.cnv_rate))::decimal(15,5) as "NAV Standard Cost",

            (c."NAV Last Direct Cost" * abs(c.cnv_rate))::decimal(15,5) as "NAV Last Direct Cost",

            (c."Cifraj Unit Cost" * abs(c.cnv_rate))::decimal(15,5) as "Cifraj Unit Cost",

            (c."Last purchase price" * abs(c.cnv_rate))::decimal(15,5) as "Last purchase price",

            (c."Legacy Unit Cost" * abs(c.cnv_rate))::decimal(15,5) as "Legacy Unit Cost",

            c."Unit Cost Type",

            (c."Unit Cost" * abs(c.cnv_rate))::decimal(15,5) as "Unit Cost",

            c."UMAS Convert Errors" || (case c.cnv_rate when -1 then 'BOM' else '' end) as "UMAS Convert Errors"

        from nav.tbl_int_prod_bom_line as a



        inner join tbl_tmp_bom_vers as b

        on a."Production BOM No_" = b."Production BOM No_" and coalesce(nullif(a."Version Code", ''), '_null') = coalesce(b."Version Code", '_null')



        inner join bom as f

        on f."Is Leaf" = false and b."Production BOM No_" = f."BOM No_" and coalesce(b."Version Code", '_null') = coalesce(f."BOM Version", '_null')



        left join lateral (

            select

                c1."No_",

                c1."Description",

                c1."Production BOM No_",

                c1."Routing No_",

                c1."Item Type",

                c1."Technical Family",

                c1."NAV Unit Cost",

                c1."NAV Standard Cost",

                c1."NAV Last Direct Cost",

                c1."Cifraj Unit Cost",

                c1."Last purchase price",

                c1."Legacy Unit Cost",

                c1."Unit Cost Type",

                c1."Unit Cost",

                c1."UMAS Convert Errors",

                (case when upper(c1."Unit of Measure") = upper(a."Unit of Measure Code") then 1 else ambi.fnc_umas_covert(c1."No_", c1."Unit of Measure", a."Unit of Measure Code") end) as cnv_rate

            from tbl_tmp_items as c1



            where c1."No_" = a."No_"

        ) as c

        on true



        left join lateral(

            select

                d1."Version Code" as "Item BOM Version",

                d1."Location Code" as "Item BOM Location"

            from tbl_tmp_bom_vers as d1

            where (case when c."Item Type" = 'MAT_PRIMA' then null else (case when a."Type" = 2 then a."No_" else c."Production BOM No_" end) end) = d1."Production BOM No_" and

                d1."Starting Date" <= current_date

            order by (case when d1."Location Code" = coalesce(b."Location Code", f."Factory") then 2 else d1.priority end) desc, d1."Starting Date" desc nulls last, d1."Last Date Modified" desc nulls last,

                d1."timestamp" desc

            limit 1

        ) as d

        on true



        left join lateral(

            select

                e1."Version Code" as "Item Route Version",

                e1."Location Code" as "Item Route Location"           

            from tbl_tmp_route_vers as e1



            where e1."Routing No_" = c."Routing No_" and e1."Starting Date" <= current_date

            order by (case when e1."Location Code" = coalesce(d."Item BOM Location", f."Factory") then 2 else e1.priority end) desc, e1."Starting Date" desc nulls last, e1."Last Date Modified" desc nulls last

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

            where g."Routing No_" = f."Routing No_" and coalesce(nullif(g."Version Code", ''), '_null') = coalesce(f."Route Version", '_null') and a."Routing Link Code" = g."Routing Link Code"

            order by g."Operation No_" asc

            limit 1

        ) as g

        on true



        where a."Starting Date" <= current_date and coalesce(nullif(a."Ending Date"::date, '1753-01-01'::date), current_date + 1::int) > current_date

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

        b."Item Type",

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

    on a."Routing No_" = b."Routing No_" and coalesce(nullif(a."Version Code", ''), '_null') = coalesce(b."Route Version", '_null')



    where b."Is Leaf" = false;



    create index tbl_mdl_bom_route_expanded_ix1 on ambi.tbl_mdl_bom_route_expanded ("Colet Item No_", "Item No_");



    /**********************************************/

    /* prepare other kit components */

    create table ambi.tbl_mdl_kit_comp_other as

    select

        a."Parent Item No_" as "Kit No_",

        b."Description" as "Kit Description",

        b."Range" as "Kit Range",

        b."Cod ABCD",

        e."First Prod_ Date" as "Kit First Prod_ Date",

        f."First Sale Date" as "Kit First Sale Date",

        a."No_" as "Item No_",

        a."Description" as "Item Description",

        c."Item Type",

        b."Factory",

        c."Technical Family" as "Item Family",

        d."First Prod_ Date" as "Item First Prod_ Date",

        d."First Sale Date" as "Item First Sale Date",

        a."Quantity per",

        a."Unit of Measure Code",

        c."Gross Weight",

        c."Net Weight",

        c."Length",

        c."Width",

        c."Height",

        c."Unit Volume",

        (case when d."Item No_" is null then c."Unit Cost Type" else 'BOM' end) as "Unit Cost Type",

        (case when d."Item No_" is null then c."Unit Cost" * abs(c.cnv_rate) else null end) as "Unit Cost",

        (case when d."Item No_" is null then c."UMAS Convert Errors" || (case c.cnv_rate when -1 then 'BOM' else '' end) else null end) as "UMAS Convert Errors"

    from nav.tbl_int_bom_component as a



    inner join tbl_tmp_items as b

    on a."Parent Item No_" = b."No_"



    left join lateral (

        select

            c1."No_",

            c1."Item Type",

            c1."Technical Family",

            c1."Gross Weight",

            c1."Net Weight",

            c1."Length",

            c1."Width",

            c1."Height",

            c1."Unit Volume",

            c1."Unit Cost Type",

            c1."Unit Cost",

            c1."UMAS Convert Errors",

            (case when upper(c1."Unit of Measure") = upper(a."Unit of Measure Code") then 1 else ambi.fnc_umas_covert(c1."No_", c1."Unit of Measure", a."Unit of Measure Code") end) as cnv_rate

        from tbl_tmp_items as c1

        where a."No_" = c1."No_"

    ) as c

    on true



    left join ambi.tbl_mdl_prod_bom_colete as d

    on c."No_" = d."Item No_"



    left join lateral (

        select

            min(e1."Posting Date"::date) as "First Prod_ Date"

        from nav.tbl_int_item_ledger_entry as e1



        where e1."Item No_" = b."No_" and e1."Entry Type" = 9 /* assembly output */



        group by e1."Item No_"

    ) as e

    on true

    

    left join lateral (

        select

            min(f1."Posting Date"::date) as "First Sale Date"

        from nav.tbl_int_item_ledger_entry as f1



        where f1."Item No_" = b."No_" and f1."Entry Type" = 1 /* sale */



        group by f1."Item No_"

    ) as f

    on true;



    /* where b."Cod ABCD" != 'D'; */



    create index tbl_mdl_kit_comp_other_ix1 on ambi.tbl_mdl_kit_comp_other("Item No_");

end;

$$ language plpgsql;

/* 0002.01 */

do $$

begin

    raise notice '************************************';

    raise notice 'creating procedure "prc_prod_bom_expanded_archv"';

end;

$$ language plpgsql;



create or replace procedure ambi.prc_prod_bom_expanded_archv(

    _bom_date date

)

as $$

begin

    /* verificare parametru */

    if _bom_date is null then

        raise exception 'Nonexistent BOM date parameter!';

    end if;



    if exists (select * from ambi.tbl_mdl_bom_comp_expanded_archv where "BOM Date" = _bom_date) then

         raise exception 'Already exists data for this BOM date!';

    end if;



    /* arhivare cifraj unit cost */

    insert into ambi.tbl_int_cifraj_unit_cost_archv ("BOM Date", item_no, descr, umas, cost, data_estim, mod_de)

    select _bom_date, item_no, descr, umas, cost, data_estim, mod_de

    from ambi.tbl_int_cifraj_unit_cost;



    /* arhivare fx rate */

    insert into ambi.tbl_int_fx_rate_archv ("BOM Date", currency, valoare, data_val)

    select _bom_date, currency, valoare, data_val

    from ambi.tbl_int_fx_rate;



    /* arhivare bom colete */

    insert into ambi.tbl_mdl_prod_bom_colete_archv ("BOM Date", "Item No_", "Description", "Range", "Item Type", "Cod ABCD", "Gross Weight", "Net Weight", "Length",

                                                    "Width", "Height", "Unit Volume", "Unit of Measure", "Last sales price", "Price Includes VAT", "NAV Unit Cost",

                                                    "NAV Standard Cost", "NAV Last Direct Cost", "BOM No_", "BOM Version", "BOM Location", "Factory", "Routing No_",

                                                    "Route Version", "Route Location","Avg Qty Type", "Avg Quantity"/*, "First Prod_ Date", "First Sale Date"*/)

    select _bom_date, "Item No_", "Description", "Range", "Item Type", "Cod ABCD", "Gross Weight", "Net Weight", "Length",

            "Width", "Height", "Unit Volume", "Unit of Measure", "Last sales price", "Price Includes VAT", "NAV Unit Cost",

            "NAV Standard Cost", "NAV Last Direct Cost", "BOM No_", "BOM Version", "BOM Location", "Factory", "Routing No_",

            "Route Version", "Route Location","Avg Qty Type", "Avg Quantity"/*, "First Prod_ Date", "First Sale Date"*/

    from ambi.tbl_mdl_prod_bom_colete;



    /* arhivare bom components expanded */

    insert into ambi.tbl_mdl_bom_comp_expanded_archv ("BOM Date", "Colet Item No_", "Colet Description", "Range", "Colet Avg Qty Type", "Colet Avg Qty", "Level",

                                                    "Line No_", "Line Type", "Is Leaf", "Item No_", "Item Description", "Factory", "BOM No_", "BOM Version", "BOM Location",

                                                    "Routing No_", "Route Version", "Route Location", "Routing Link Code", "Item Type", "Item Family", "Quantity per",

                                                    "Quantity", "Scrap _", "Parent Route Fixed Scrap Qty", "Parent Route Scrap Factor", "Parent Route Fixed Scrap Qty Accum",

                                                    "Parent Route Scrap Factor Accum", "Unit of Measure Code", "Quantity w Scrap", "NAV Unit Cost", "NAV Standard Cost",

                                                    "NAV Last Direct Cost", "Cifraj Unit Cost", "Last purchase price", "Legacy Unit Cost", "Unit Cost Type", "Unit Cost",

                                                    "UMAS Convert Errors")

    select _bom_date, "Colet Item No_", "Colet Description", "Range", "Colet Avg Qty Type", "Colet Avg Qty", "Level",

            "Line No_", "Line Type", "Is Leaf", "Item No_", "Item Description", "Factory", "BOM No_", "BOM Version", "BOM Location",

            "Routing No_", "Route Version", "Route Location", "Routing Link Code", "Item Type", "Item Family", "Quantity per",

            "Quantity", "Scrap _", "Parent Route Fixed Scrap Qty", "Parent Route Scrap Factor", "Parent Route Fixed Scrap Qty Accum",

            "Parent Route Scrap Factor Accum", "Unit of Measure Code", "Quantity w Scrap", "NAV Unit Cost", "NAV Standard Cost",

            "NAV Last Direct Cost", "Cifraj Unit Cost", "Last purchase price", "Legacy Unit Cost", "Unit Cost Type", "Unit Cost",

            "UMAS Convert Errors"

    from ambi.tbl_mdl_bom_comp_expanded;



    /* arhivare bom routing expanded */

    insert into ambi.tbl_mdl_bom_route_expanded_archv ("BOM Date", "Colet Item No_", "Colet Description", "Colet Avg Qty Type", "Colet Avg Qty", "Level", "Line No_", "Item No_",

                                                        "Item Description", "Item Type", "Factory", "BOM No_", "BOM Version", "BOM Location", "Quantity w Scrap", "Unit of Measure Code",

                                                        "Routing No_", "Route Version", "Route Location", "Operation No_", "Next Operation No_", "Previous Operation No_", "Type", "No_",

                                                        "Description", "Work Center No_", "Work Center Group Code", "Setup Time", "Setup Time Unit of Meas_ Code", "Run Time",

                                                        "Run Time Unit of Meas_ Code", "Routing Link Code", "Machine Efficiency (_)", "No_ Of Workers", "Machine Time", "Workers Time")

    select _bom_date, "Colet Item No_", "Colet Description", "Colet Avg Qty Type", "Colet Avg Qty", "Level", "Line No_", "Item No_",

            "Item Description", "Item Type", "Factory", "BOM No_", "BOM Version", "BOM Location", "Quantity w Scrap", "Unit of Measure Code",

            "Routing No_", "Route Version", "Route Location", "Operation No_", "Next Operation No_", "Previous Operation No_", "Type", "No_",

            "Description", "Work Center No_", "Work Center Group Code", "Setup Time", "Setup Time Unit of Meas_ Code", "Run Time",

            "Run Time Unit of Meas_ Code", "Routing Link Code", "Machine Efficiency (_)", "No_ Of Workers", "Machine Time", "Workers Time"

    from ambi.tbl_mdl_bom_route_expanded;



    /* arhivare kit bom components */

    insert into ambi.tbl_mdl_kit_comp_other_archv ("BOM Date", "Kit No_", "Kit Description", "Kit Range", "Cod ABCD", "Item No_", "Item Description", "Item Type",

                                                    "Factory", "Item Family", "Quantity per", "Unit of Measure Code", "Gross Weight", "Net Weight", "Length", "Width",

                                                    "Height", "Unit Volume", "Unit Cost Type", "Unit Cost", "UMAS Convert Errors"/*,

                                                    "Kit First Prod_ Date", "Kit First Sale Date", "Item First Prod_ Date", "Item First Sale Date"*/)

    select _bom_date, "Kit No_", "Kit Description", "Kit Range", "Cod ABCD", "Item No_", "Item Description", "Item Type",

            "Factory", "Item Family", "Quantity per", "Unit of Measure Code", "Gross Weight", "Net Weight", "Length", "Width",

            "Height", "Unit Volume", "Unit Cost Type", "Unit Cost", "UMAS Convert Errors"/*,

            "Kit First Prod_ Date", "Kit First Sale Date", "Item First Prod_ Date", "Item First Sale Date"*/

    from ambi.tbl_mdl_kit_comp_other;

end;

$$ language plpgsql;

/* 0003.01 */

do $$

begin

    raise notice '************************************';

    raise notice 'creating procedure "prc_gl_entry_reconciliation_by_day"';

end;

$$ language plpgsql;



create or replace procedure ambi.prc_gl_entry_reconciliation_by_day(

    _date date

) as $$

declare

    _contor int;

begin

    drop table if exists tbl_tmp_gl_entry;

    drop table if exists tbl_tmp_item_transaction_list;

    drop table if exists tbl_tmp_nonitem_transaction_list;

    drop table if exists tbl_tmp_transaction_reconciliation;

    drop table if exists tbl_tmp_recs_placeholder;

    drop table if exists tbl_tmp_nonitem_trans_1D;

    drop table if exists tbl_tmp_nonitem_trans_1C;

    drop table if exists tbl_tmp_item_entry_links;



    /* obtine inregistrarile pentru perioada */

    /***************************************************/

    create temp table tbl_tmp_gl_entry on commit drop as

    select

        *

    from nav.tbl_int_gl_entry as a

    where a."Posting Date"::date = _date;



    create index if not exists tbl_tmp_gl_entry_ix1 on tbl_tmp_gl_entry ("Transaction No_");

    create index if not exists tbl_tmp_gl_entry_ix2 on tbl_tmp_gl_entry ("Entry No_");

    create index if not exists tbl_tmp_gl_entry_ix3 on tbl_tmp_gl_entry ("Transaction No_", "Entry No_");



    /* stergere inregistrari care nu au nici o valoare */

    delete from tbl_tmp_gl_entry as a

    where a."Debit Amount" = 0 and a."Credit Amount" = 0;



    /* iesire daca nu exista inrgistrari pentru perioada */

    if not exists (select * from tbl_tmp_gl_entry) then

        return;

    end if;



    /* stabilire tranzactii care au link cu value entry */

    /***************************************************/

    create temp table if not exists tbl_tmp_item_transaction_list on commit drop as

    select distinct

        b1."Transaction No_"

    from tbl_tmp_gl_entry as b1



    inner join nav.tbl_int_gl_item_ledger_relation as b2

    on b1."Entry No_" = b2."G_L Entry No_";



    /* stabilire tranzactii care nu au link cu value entry */

    /***************************************************/

    create temp table if not exists tbl_tmp_nonitem_transaction_list on commit drop as

    select distinct

        a."Transaction No_"

    from tbl_tmp_gl_entry as a



    left join tbl_tmp_item_transaction_list as b

    on a."Transaction No_" = b."Transaction No_"



    where b."Transaction No_" is null

    order by a."Transaction No_" asc;



    /* creare tabela care va contine inregistrarile reconciliate */

    /***************************************************/

    create temp table if not exists tbl_tmp_transaction_reconciliation (

        "Posting Date" date not null,

        tip text not null,

        "Transaction No_" int not null,

        "Entry No_" int not null,

        "G_L Account No_" text not null,

        "Bal_ Entry No_" int,

        "Bal_ Account No_" text,

        "Source Type" int,

        "Source No_" text,

        "Debit Amount" decimal(38, 20) not null,

        "Credit Amount" decimal(38, 20) not null,

        "Debit Amount (FCY)" decimal(38, 20) not null,

        "Credit Amount (FCY)" decimal(38, 20) not null,

        "Currency Code" text not null,

        item_link text

    ) on commit drop;

    create unique index if not exists tbl_tmp_transaction_reconciliation_ix1 on tbl_tmp_transaction_reconciliation ("Transaction No_", "Entry No_", "Bal_ Entry No_");



    /* creare tabela pentru reconciliere inregistrari 1 la 1 */

    /***************************************************/

    create unlogged table if not exists tbl_tmp_recs_placeholder (

        "Posting Date" date not null,

        "Transaction No_" int not null,

        "Entry No_" int not null,

        "G_L Account No_" text not null,

        "Source Type" int,

        "Source No_" text,

        "Debit Amount" decimal(38, 20) not null,

        "Credit Amount" decimal(38, 20) not null,

        "Debit Amount (FCY)" decimal(38, 20) not null,

        "Credit Amount (FCY)" decimal(38, 20) not null,

        "Currency Code" text not null,

        "Transaction No_ 2" int not null,

        "Entry No_ 2" int not null,

        "G_L Account No_ 2" text not null,

        "Source Type 2" int,

        "Source No_ 2" text,

        "Debit Amount 2" decimal(38, 20) not null,

        "Credit Amount 2" decimal(38, 20) not null,

        "Debit Amount (FCY) 2" decimal(38, 20) not null,

        "Credit Amount (FCY) 2" decimal(38, 20) not null,

        "Currency Code 2" text not null,

        item_link text

    );



    /* bucla reconciliere inregistrari 1 la 1 */

    /***************************************************/

    _contor := 0;

    loop

        with transactions as (

            select

                a."Transaction No_"

            from tbl_tmp_gl_entry as a



            inner join tbl_tmp_nonitem_transaction_list as b

            on a."Transaction No_" = b."Transaction No_"



            group by a."Transaction No_"

            having count(*) not in (1, 3)

        )

        insert into tbl_tmp_recs_placeholder ("Posting Date", "Transaction No_", "Entry No_", "G_L Account No_", "Source Type", "Source No_",

                                                "Debit Amount", "Credit Amount", "Debit Amount (FCY)", "Credit Amount (FCY)", "Currency Code",

                                                "Transaction No_ 2", "Entry No_ 2", "G_L Account No_ 2", "Source Type 2", "Source No_ 2",

                                                "Debit Amount 2", "Credit Amount 2", "Debit Amount (FCY) 2", "Credit Amount (FCY) 2", "Currency Code 2")

        select distinct on (a."Transaction No_")

            a."Posting Date"::date,

            a."Transaction No_",

            a."Entry No_",

            a."G_L Account No_",

            nullif(a."Source Type", 0) as "Source Type",

            nullif(a."Source No_", '') as "Source No_",

            a."Debit Amount",

            a."Credit Amount",

            a."Debit Amount (FCY)",

            a."Credit Amount (FCY)",

            a."Currency Code",

            c."Transaction No_ 2",

            c."Entry No_ 2",

            c."G_L Account No_ 2",

            c."Source Type 2",

            c."Source No_ 2",

            c."Debit Amount 2",

            c."Credit Amount 2",

            c."Debit Amount (FCY) 2",

            c."Credit Amount (FCY) 2",

            c."Currency Code 2"

        from tbl_tmp_gl_entry as a



        inner join transactions as b

        on a."Transaction No_" = b."Transaction No_"



        inner join lateral (

            select distinct on (c1."Transaction No_")

                c1."Transaction No_" as "Transaction No_ 2",

                c1."Entry No_" as "Entry No_ 2",

                c1."G_L Account No_" as "G_L Account No_ 2",

                nullif(c1."Source Type", 0) as "Source Type 2",

                nullif(c1."Source No_", '') as "Source No_ 2",

                c1."Debit Amount" as "Debit Amount 2",

                c1."Credit Amount" as "Credit Amount 2",

                c1."Debit Amount (FCY)" as "Debit Amount (FCY) 2",

                c1."Credit Amount (FCY)" as "Credit Amount (FCY) 2",

                c1."Currency Code" as "Currency Code 2"

            from tbl_tmp_gl_entry as c1



            where c1."Transaction No_" = a."Transaction No_" and c1."Entry No_" > a."Entry No_" and a."G_L Account No_" != c1."G_L Account No_" and

                ((a."Debit Amount" != 0 and c1."Credit Amount" = a."Debit Amount") or (a."Credit Amount"!= 0 and c1."Debit Amount" = a."Credit Amount"))



            order by c1."Transaction No_" asc, c1."Entry No_" asc

        ) as c

        on true



        order by a."Transaction No_" asc, a."Entry No_" asc;



        /* notificare contor */

        _contor := _contor + 1;

        if _contor % 10 = 0 then

            raise notice 'Non item related reconciliation for % reached % loops', _date, _contor;

        end if;



        /* iesire cand nu se mai gasesc inregistrari pereche */

        exit when (select count(*) from tbl_tmp_recs_placeholder) = 0;



        /* salvare inregistrari pereche */

        insert into tbl_tmp_transaction_reconciliation ("Posting Date", tip, "Transaction No_", "Entry No_", "G_L Account No_", "Bal_ Entry No_", "Bal_ Account No_", "Source Type", "Source No_",

                                                        "Debit Amount", "Credit Amount", "Debit Amount (FCY)", "Credit Amount (FCY)", "Currency Code")

        select

            a."Posting Date",

            '1D - 1C' as tip,

            a."Transaction No_",

            a."Entry No_",

            a."G_L Account No_",

            a."Entry No_ 2",

            a."G_L Account No_ 2",

            coalesce(a."Source Type", a."Source Type 2"),

            coalesce(a."Source No_", a."Source No_ 2"),

            a."Debit Amount",

            a."Credit Amount",

            a."Debit Amount (FCY)",

            a."Credit Amount (FCY)",

            a."Currency Code"

        from tbl_tmp_recs_placeholder as a



        union all



        select

            b."Posting Date",

            '1D - 1C' as tip,

            b."Transaction No_ 2",

            b."Entry No_ 2",

            b."G_L Account No_ 2",

            b."Entry No_",

            b."G_L Account No_",

            coalesce(b."Source Type 2", b."Source Type"),

            coalesce(b."Source No_ 2", b."Source No_"),

            b."Debit Amount 2",

            b."Credit Amount 2",

            b."Debit Amount (FCY) 2",

            b."Credit Amount (FCY) 2",

            b."Currency Code 2"

        from tbl_tmp_recs_placeholder as b;



        /* sterge inregsitrari pereche din tabela cu inregistrari pentru perioada */

        delete from tbl_tmp_gl_entry as a

        using tbl_tmp_recs_placeholder as b

        where a."Entry No_" in (b."Entry No_", b."Entry No_ 2");



        /* golire tabela reconciliere inregistrari 1 la 1 */

        truncate table tbl_tmp_recs_placeholder;

    end loop;



    /* verificare inregistrari ramase fara link in value entry sunt ok */

    /***************************************************/

    if exists (

        select

            *

        from (

            select

                a1."Transaction No_",

                count(1) filter (where a1."Debit Amount" != 0) as debit_poz,

                count(1) filter (where a1."Credit Amount" != 0) as credit_poz,

                sum(a1."Debit Amount") as debit_amount,

                sum(a1."Credit Amount") as credit_amount

            from tbl_tmp_gl_entry as a1



            inner join tbl_tmp_nonitem_transaction_list as a2

            on a1."Transaction No_" = a2."Transaction No_"



            group by a1."Transaction No_"

        ) as a



        where a.debit_amount != a.credit_amount

    ) then

        raise exception 'Inregistrarile ramase dupa reconcilierea 1 la 1, fara link in value entry, nu au aceeasi valoare pe debit si credit !';

    end if;



    /* salvare lista tranzactii fara link in value entry cu o singura valoare pe debit */

    /***************************************************/

    create temp table tbl_tmp_nonitem_trans_1D on commit drop as

    select

        a."Transaction No_"

    from tbl_tmp_gl_entry as a



    inner join tbl_tmp_nonitem_transaction_list as b

    on a."Transaction No_" = b."Transaction No_"



    group by a."Transaction No_"

    having count(1) filter (where a."Debit Amount" != 0) = 1;



    /* salvare inregistrari 1 debit la multi credit */

    /***************************************************/

    with debit_1_recs_debit as (

        select

            a.*

        from tbl_tmp_gl_entry as a



        inner join tbl_tmp_nonitem_trans_1D as b

        on a."Transaction No_" = b."Transaction No_"



        where a."Debit Amount" != 0

    ),

    debit_1_recs_credit as (

        select

            a.*

        from tbl_tmp_gl_entry as a



        inner join tbl_tmp_nonitem_trans_1D as b

        on a."Transaction No_" = b."Transaction No_"



        where a."Credit Amount" != 0

    )

    insert into tbl_tmp_transaction_reconciliation ("Posting Date", tip, "Transaction No_", "Entry No_", "G_L Account No_", "Bal_ Entry No_", "Bal_ Account No_", "Source Type", "Source No_",

                                                    "Debit Amount", "Credit Amount", "Debit Amount (FCY)", "Credit Amount (FCY)", "Currency Code")

    select

        a."Posting Date"::date,

        '1D - nC' as tip,

        a."Transaction No_",

        a."Entry No_",

        a."G_L Account No_",

        b."Entry No_" as "Bal_ Entry No_",

        b."G_L Account No_" as "Bal_ Account No_",

        coalesce(nullif(a."Source Type", 0), b."Source Type") as "Source Type",

        coalesce(nullif(a."Source No_", ''), b."Source No_") as "Source No_",

        b."Credit Amount" as "Debit Amount",

        0::decimal as "Credit Amount",

        b."Credit Amount (FCY)" as "Debit Amount (FCY)",

        0::decimal as "Credit Amount (FCY)",

        b."Currency Code"

    from debit_1_recs_debit as a



    inner join debit_1_recs_credit as b

    on a."Transaction No_" = b."Transaction No_"



    union all



    select

        b."Posting Date",

        'nC - 1D' as tip,

        b."Transaction No_",

        b."Entry No_",

        b."G_L Account No_",

        a."Entry No_" as "Bal_ Entry No_",

        a."G_L Account No_" as "Bal_ Account No_",

        coalesce(nullif(b."Source Type", 0), a."Source Type") as "Source Type",

        coalesce(nullif(b."Source No_", ''), a."Source No_") as "Source No_",

        0::decimal as "Debit Amount",

        b."Credit Amount",

        0::decimal as "Debit Amount (FCY)",

        b."Credit Amount (FCY)",

        b."Currency Code"

    from debit_1_recs_debit as a



    inner join debit_1_recs_credit as b

    on a."Transaction No_" = b."Transaction No_";



    /* stergere tranzactii 1 debit la multi credit din gl entries */

    delete from tbl_tmp_gl_entry as a

    using tbl_tmp_nonitem_trans_1D as b

    where a."Transaction No_" = b."Transaction No_";



    drop table if exists tbl_tmp_nonitem_trans_1D;



    /* salvare lista tranzactii fara link in value entry cu o singura valoare pe credit */

    /***************************************************/

    create temp table tbl_tmp_nonitem_trans_1C on commit drop as

    select

        a."Transaction No_"

    from tbl_tmp_gl_entry as a



    inner join tbl_tmp_nonitem_transaction_list as b

    on a."Transaction No_" = b."Transaction No_"



    group by a."Transaction No_"

    having count(1) filter (where a."Credit Amount" != 0) = 1;



    /* salvare inregistrari 1 credit la multi debit */

    /***************************************************/

    with debit_1_recs_credit as (

        select

            a.*

        from tbl_tmp_gl_entry as a



        inner join tbl_tmp_nonitem_trans_1C as b

        on a."Transaction No_" = b."Transaction No_"



        where a."Credit Amount" != 0

    ),

    debit_1_recs_debit as (

        select

            a.*

        from tbl_tmp_gl_entry as a



        inner join tbl_tmp_nonitem_trans_1C as b

        on a."Transaction No_" = b."Transaction No_"



        where a."Debit Amount" != 0

    )

    insert into tbl_tmp_transaction_reconciliation ("Posting Date", tip, "Transaction No_", "Entry No_", "G_L Account No_", "Bal_ Entry No_", "Bal_ Account No_", "Source Type", "Source No_",

                                                    "Debit Amount", "Credit Amount", "Debit Amount (FCY)", "Credit Amount (FCY)", "Currency Code")

    select

        a."Posting Date"::date,

        '1C - nD' as tip,

        a."Transaction No_",

        a."Entry No_",

        a."G_L Account No_",

        b."Entry No_" as "Bal_ Entry No_",

        b."G_L Account No_" as "Bal_ Account No_",

        coalesce(nullif(a."Source Type", 0), b."Source Type") as "Source Type",

        coalesce(nullif(a."Source No_", ''), b."Source No_") as "Source No_",

        0::decimal as "Debit Amount",

        b."Debit Amount" as "Credit Amount",

        0::decimal as "Debit Amount (FCY)",

        b."Debit Amount (FCY)" as "Credit Amount (FCY)",

        b."Currency Code"

    from debit_1_recs_credit as a



    inner join debit_1_recs_debit as b

    on a."Transaction No_" = b."Transaction No_"



    union all



    select

        b."Posting Date"::date,

        'nD - 1C' as tip,

        b."Transaction No_",

        b."Entry No_",

        b."G_L Account No_",

        a."Entry No_" as "Bal_ Entry No_",

        a."G_L Account No_" as "Bal_ Account No_",

        coalesce(nullif(b."Source Type", 0), a."Source Type") as "Source Type",

        coalesce(nullif(b."Source No_", ''), a."Source No_") as "Source No_",

        b."Debit Amount",

        0::decimal as "Credit Amount",

        b."Debit Amount (FCY)",

        0::decimal as "Credit Amount (FCY)",

        b."Currency Code"

    from debit_1_recs_credit as a



    inner join debit_1_recs_debit as b

    on a."Transaction No_" = b."Transaction No_";



    /* stergere tranzactii 1 credit la multi debit din gl entries */

    delete from tbl_tmp_gl_entry as a

    using tbl_tmp_nonitem_trans_1C as b

    where a."Transaction No_" = b."Transaction No_";



    drop table if exists tbl_tmp_nonitem_trans_1C;



    /* introducere si stergere inregistrari fara link in value entry care nu au doar o valoare pe debit sau credit */

    /***************************************************/

    with entries as(

        insert into tbl_tmp_transaction_reconciliation ("Posting Date", tip, "Transaction No_", "Entry No_", "G_L Account No_", "Bal_ Entry No_", "Bal_ Account No_", "Source Type", "Source No_",

                                                    "Debit Amount", "Credit Amount", "Debit Amount (FCY)", "Credit Amount (FCY)", "Currency Code")

        select

            b1."Posting Date"::date,

            'nonItem error' as tip,

            b1."Transaction No_",

            b1."Entry No_",

            b1."G_L Account No_",

            null::int as "Bal_ Entry No_",

            null::text as "Bal_ Account No_",

            b1."Source Type",

            b1."Source No_",

            b1."Debit Amount",

            b1."Credit Amount",

            b1."Debit Amount (FCY)",

            b1."Credit Amount (FCY)",

            b1."Currency Code"

        from tbl_tmp_gl_entry as b1

        inner join tbl_tmp_nonitem_transaction_list as b2

        on b1."Transaction No_" = b2."Transaction No_"



        returning "Transaction No_", "Entry No_"

    )

    delete from tbl_tmp_gl_entry as a

    using entries as b

    where a."Transaction No_" = b."Transaction No_" and a."Entry No_" = b."Entry No_";



    /* verificare corectitudine inregistrari */

    /***************************************************/

    if exists (

        select

            a."Transaction No_"

        from tbl_tmp_transaction_reconciliation as a

        group by a."Transaction No_"

        having sum(a."Debit Amount") != sum(a."Credit Amount")

    ) then

        raise exception 'Inregsitrarile fara link in value entry, reconciliate, nu au aceleasi valori pe debit si credit!';

    end if;



    if exists(

        select

            *

        from tbl_tmp_gl_entry as a



        inner join tbl_tmp_nonitem_transaction_list as b

        on a."Transaction No_" = b."Transaction No_"

    ) then

        raise exception 'Mai exista inregistrari fara link in value entry de reconciliat!';

    end if;



    if exists (

        select

            a."Transaction No_"                

        from tbl_tmp_gl_entry as a

        group by a."Transaction No_"

        having sum(a."Debit Amount") != sum(a."Credit Amount")

    ) then

        raise exception 'Tranzactiile ramase, dupa reconcilierea celor fara link in value entry, nu au aceleasi valori pe debit si credit!';

    end if;



    if exists (

        select

            *

        from tbl_tmp_gl_entry as a



        left join nav.tbl_int_gl_item_ledger_relation as b

        on a."Entry No_" = b."G_L Entry No_"



        where b."G_L Entry No_" is null

    ) then

        raise exception 'Inca mai exista pozitii in gl entries fara link in value entry!';

    end if;



    /* stabilire lista link-uri gl entry catre value entry */

    /***************************************************/

    create temp table tbl_tmp_item_entry_links on commit drop as

    select

        a."Transaction No_",

        a."Entry No_",

        string_agg(b."Value Entry No_"::text, ',' order by b."Value Entry No_" asc) as link

    from tbl_tmp_gl_entry as a

    inner join nav.tbl_int_gl_item_ledger_relation as b

    on a."Entry No_" = b."G_L Entry No_"



    group by a."Transaction No_", a."Entry No_"



    order by a."Transaction No_" asc, a."Entry No_" asc;



    /* bucla reconciliere inregistrari cu link in value entry */

    /***************************************************/

    truncate table tbl_tmp_recs_placeholder;

    _contor := 0;

    loop

        /* imperechere inregistrari cu acelasi link in value entry */

        insert into tbl_tmp_recs_placeholder ("Posting Date", "Transaction No_", "Entry No_", "G_L Account No_", "Source Type", "Source No_",

                                                "Debit Amount", "Credit Amount", "Debit Amount (FCY)", "Credit Amount (FCY)", "Currency Code",

                                                "Transaction No_ 2", "Entry No_ 2", "G_L Account No_ 2", "Source Type 2", "Source No_ 2",

                                                "Debit Amount 2", "Credit Amount 2", "Debit Amount (FCY) 2", "Credit Amount (FCY) 2", "Currency Code 2",

                                                item_link)

        select distinct on (a.link)

            b."Posting Date"::date,

            a."Transaction No_",

            a."Entry No_",

            b."G_L Account No_",

            nullif(b."Source Type", 0) as "Source Type",

            nullif(b."Source No_", '') as "Source No_",

            b."Debit Amount",

            b."Credit Amount",

            b."Debit Amount (FCY)",

            b."Credit Amount (FCY)",

            b."Currency Code",

            c."Transaction No_ 2",

            c."Entry No_ 2",

            c."G_L Account No_ 2",

            c."Source Type 2",

            c."Source No_ 2",

            c."Debit Amount 2",

            c."Credit Amount 2",

            c."Debit Amount (FCY) 2",

            c."Credit Amount (FCY) 2",

            c."Currency Code 2",

            a.link

        from tbl_tmp_item_entry_links as a



        inner join tbl_tmp_gl_entry as b

        on a."Transaction No_" = b."Transaction No_" and a."Entry No_" = b."Entry No_"



        inner join lateral (

            select distinct on (c1.link)

                c1."Transaction No_" as "Transaction No_ 2",

                c1."Entry No_" as "Entry No_ 2",

                c2."G_L Account No_" as "G_L Account No_ 2",

                nullif(c2."Source Type", 0) as "Source Type 2",

                nullif(c2."Source No_", '') as "Source No_ 2",

                c2."Debit Amount" as "Debit Amount 2",

                c2."Credit Amount" as "Credit Amount 2",

                c2."Debit Amount (FCY)" as "Debit Amount (FCY) 2",

                c2."Credit Amount (FCY)" as "Credit Amount (FCY) 2",

                c2."Currency Code" as "Currency Code 2"

            from tbl_tmp_item_entry_links as c1



            inner join tbl_tmp_gl_entry as c2

            on c1."Transaction No_" = c2."Transaction No_" and c1."Entry No_" = c2."Entry No_"



            where c1.link = a.link and c1."Transaction No_" = a."Transaction No_" and c1."Entry No_" > a."Entry No_" and c2."G_L Account No_" != b."G_L Account No_" and

                ((b."Debit Amount" != 0 and b."Debit Amount" = c2."Credit Amount") or (b."Credit Amount" != 0 and b."Credit Amount" = c2."Debit Amount"))



            order by c1.link asc, c1."Transaction No_" asc, c1."Entry No_" asc

        ) as c

        on true



        order by a.link asc, a."Transaction No_" asc, a."Entry No_" asc;



        /* notificare contor */

        _contor := +_contor + 1;

        if _contor % 10 = 0 then

            raise notice 'Item related reconciliation loop for % reached %', _date, _contor;

        end if;



        /* iesire cand nu se mai gasesc inregistrari pereche */

        exit when (select count(*) from tbl_tmp_recs_placeholder) = 0;



        /* salvare inregistrari pereche */

        insert into tbl_tmp_transaction_reconciliation ("Posting Date", tip, "Transaction No_", "Entry No_", "G_L Account No_", "Bal_ Entry No_", "Bal_ Account No_", "Source Type", "Source No_",

                                                        "Debit Amount", "Credit Amount", "Debit Amount (FCY)", "Credit Amount (FCY)", "Currency Code", item_link)

        select

            a."Posting Date",

            'item link' as tip,

            a."Transaction No_",

            a."Entry No_",

            a."G_L Account No_",

            a."Entry No_ 2",

            a."G_L Account No_ 2",

            coalesce(a."Source Type", a."Source Type 2"),

            coalesce(a."Source No_", a."Source No_ 2"),

            a."Debit Amount",

            a."Credit Amount",

            a."Debit Amount (FCY)",

            a."Credit Amount (FCY)",

            a."Currency Code",

            item_link

        from tbl_tmp_recs_placeholder as a



        union all



        select

            b."Posting Date",

            'item link' as tip,

            b."Transaction No_ 2",

            b."Entry No_ 2",

            b."G_L Account No_ 2",

            b."Entry No_",

            b."G_L Account No_",

            coalesce(b."Source Type 2", b."Source Type"),

            coalesce(b."Source No_ 2", b."Source No_"),

            b."Debit Amount 2",

            b."Credit Amount 2",

            b."Debit Amount (FCY) 2",

            b."Credit Amount (FCY) 2",

            b."Currency Code 2",

            b.item_link

        from tbl_tmp_recs_placeholder as b;



        /* sterge inregsitrari pereche din tabela cu inregistrari pentru perioada */

        delete from tbl_tmp_gl_entry as a

        using tbl_tmp_recs_placeholder as b

        where a."Entry No_" in (b."Entry No_", b."Entry No_ 2");



        /* golire tabela reconciliere inregistrari 1 la 1 */

        truncate table tbl_tmp_recs_placeholder;

    end loop;



    drop table tbl_tmp_recs_placeholder;



    /* introducere si stergere inregistrari fara link in value entry nereconciliate */

    /***************************************************/

    with entries as(

        insert into tbl_tmp_transaction_reconciliation ("Posting Date", tip, "Transaction No_", "Entry No_", "G_L Account No_", "Bal_ Entry No_", "Bal_ Account No_", "Source Type", "Source No_",

                                                    "Debit Amount", "Credit Amount", "Debit Amount (FCY)", "Credit Amount (FCY)", "Currency Code")

        select

            b1."Posting Date"::date,

            'Item error' as tip,

            b1."Transaction No_",

            b1."Entry No_",

            b1."G_L Account No_",

            null::int as "Bal_ Entry No_",

            null::text as "Bal_ Account No_",

            b1."Source Type",

            b1."Source No_",

            b1."Debit Amount",

            b1."Credit Amount",

            b1."Debit Amount (FCY)",

            b1."Credit Amount (FCY)",

            b1."Currency Code"

        from tbl_tmp_gl_entry as b1

        inner join tbl_tmp_item_transaction_list as b2

        on b1."Transaction No_" = b2."Transaction No_"



        returning "Transaction No_", "Entry No_"

    )

    delete from tbl_tmp_gl_entry as a

    using entries as b

    where a."Transaction No_" = b."Transaction No_" and a."Entry No_" = b."Entry No_";



    /* verificare corectitudine inregistrari */

    /***************************************************/

    if exists (

        select

            a."Transaction No_"

        from tbl_tmp_transaction_reconciliation as a

        inner join tbl_tmp_item_transaction_list as b

        on a."Transaction No_" = b."Transaction No_"

        group by a."Transaction No_"

        having sum(a."Debit Amount") != sum(a."Credit Amount")

    ) then

        raise exception 'Inregsitrarile cu link in value entry, reconciliate, nu au aceleasi valori pe debit si credit!';

    end if;



    if exists (select * from tbl_tmp_gl_entry) then

        raise exception 'Mai exista inregistrari nereconciliate!';

    end if;



    /* salvare inregistrari reconciliate */

    /***************************************************/

    delete from ambi.tbl_mdl_gl_entry_reconciliation as a

    where a."Posting Date" = _date;



    insert into ambi.tbl_mdl_gl_entry_reconciliation ("Posting Date", tip, "Transaction No_", "Entry No_", "G_L Account No_",

                                                    "Bal_ Entry No_", "Bal_ Account No_", "Source Type", "Source No_",

                                                    "Debit Amount", "Credit Amount", "Debit Amount (FCY)", "Credit Amount (FCY)", "Currency Code",

                                                    item_link)

    select

        "Posting Date", tip, "Transaction No_", "Entry No_", "G_L Account No_",

        "Bal_ Entry No_", "Bal_ Account No_", "Source Type", "Source No_",

        "Debit Amount", "Credit Amount", "Debit Amount (FCY)", "Credit Amount (FCY)", "Currency Code",

        item_link

    from tbl_tmp_transaction_reconciliation;

end;

$$ language plpgsql;



/* 0003.02 */

do $$

begin

    raise notice '************************************';

    raise notice 'creating procedure "prc_gl_entry_reconciliation_by_month"';

end;

$$ language plpgsql;



create or replace procedure ambi.prc_gl_entry_reconciliation_by_month(

    _year int,

    _month int,

    _start_day int,

    _end_day int

) as $$

declare

    _start_time timestamp;

begin

    if _year not between 2000 and 9999 then

        raise exception 'Anul furnizat nu este ok';

    end if;



    if _month not between 1 and 12 then

        raise exception 'Luna furnizata nu este ok';

    end if;



    if _start_day not between 1 and 31 then

        raise exception 'Ziua de inceput furnizata nu este ok';

    end if;



    if _end_day not between 1 and 31 and _end_day < _start_day then

        raise exception 'Ziua de sfarsit furnizata nu este ok';

    end if;



    for _day in _start_day.._end_day loop

        raise notice '********************************************';

        raise notice 'Procesarea a inceput pentru data %', make_date(_year, _month, _day);

        raise notice '********************************************';



        _start_time := timeofday()::timestamp;

        call ambi.prc_gl_entry_reconciliation_by_day (make_date(_year, _month, _day));



        raise notice '********************************************';

        raise notice 'Procesarea pentru data % a durat % secunde', make_date(_year, _month, _day), extract(epoch from (timeofday()::timestamp - _start_time))::int;

        raise notice '********************************************';

    end loop;

end; 

$$ language plpgsql;