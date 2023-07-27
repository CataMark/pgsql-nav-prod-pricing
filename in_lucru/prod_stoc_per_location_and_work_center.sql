do $$
declare
    _locations json := '[{"loc":"AIT","compart":"PROD%"}]'::json;
    _wrkcentr_compart text := 'PROD';
begin
    drop table if exists tbl_tmp_stoc;
    drop table if exists tbl_tmp_bom;
    drop table if exists tbl_tmp_routing;
    drop table if exists tbl_tmp_rezultat;

    /* stoc table */
    /************************************/
    create temp table tbl_tmp_stoc on commit drop as
    select
        a."Location Code",
        a."Bin Code",
        a."Item No_",
        a."Description",
        a."Inventory Posting Group",
        a."Range",
        b."Whse_ Document Type",
        b."Last Doc_ Type Name",
        b."Source Document",
        b."Last Source Doc_ Name",
        b."Source No_",
        b."Source Line No_",
        a."Unit of Measure Code",
        a."Quantity",
        a."Unit Cost",
        round(a."Quantity" * a."Unit Cost", 2) as "Total Cost"
    from (
        select
            a1."Location Code",
            a1."Bin Code",
            a1."Item No_",
            a2."Description",
            a2."Inventory Posting Group",
            a2."Range",
            a1."Unit of Measure Code",
            round(sum(a1."Quantity"), 2) as "Quantity",
            round(a2."Unit Cost", 2) as "Unit Cost"
        from nav.tbl_int_warehouse_entry as a1

        left join nav.tbl_int_item as a2
        on a1."Item No_" = a2."No_"

        inner join json_to_recordset(_locations) as c(loc text, compart text)
        on a1."Location Code" = c.loc and a1."Bin Code" like c.compart

        group by a1."Location Code",
            a1."Bin Code",
            a1."Item No_",
            a2."Description",
            a2."Inventory Posting Group",
            a2."Range",
            a1."Unit of Measure Code",
            a2."Unit Cost") as a

    left join lateral (
        select distinct on (b1."Item No_")
            b1."Whse_ Document Type",
            b2."Description" as "Last Doc_ Type Name",
            b1."Source Document",
            b3."Description" as "Last Source Doc_ Name",
            (case b1."Whse_ Document Type" when 5 then b1."Source No_" else null end) as "Source No_",
            (case b1."Whse_ Document Type" when 5 then b1."Source Line No_" else null end) as "Source Line No_"
        from nav.tbl_int_warehouse_entry as b1

        left join nav.tbl_int_warehouse_entry_whse_document_type as b2
        on b1."Whse_ Document Type" = b2."Whse_ Document Type"

        left join nav.tbl_int_warehouse_entry_source_document as b3
        on b1."Source Document" = b3."Source Document"

        where a."Bin Code" = _wrkcentr_compart and b1."Bin Code" = a."Bin Code" and b1."Item No_" = a."Item No_" and b1."Location Code" = a."Location Code"

        order by b1."Item No_" asc, (case b1."Whse_ Document Type" when 5 then 0 else 1 end) asc, b1."Registering Date" desc, b1."Entry No_" desc
    ) as b
    on true

    where a."Quantity" != 0;

    create index if not exists tbl_tmp_stoc_ix1 on tbl_tmp_stoc ("Item No_", "Location Code", "Bin Code");
    create index if not exists tbl_tmp_stoc_ix2 on tbl_tmp_stoc ("Item No_", "Source No_", "Source Line No_");

    /* bom table */
    /************************************/
    create temp table tbl_tmp_bom on commit drop as
    with recursive bom as(
        select
            a."Production BOM No_",
            a."Version Code",
            b."Location Code",
            a."No_",
            a."Routing Link Code"
        from nav.tbl_int_prod_bom_line as a

        left join nav.tbl_int_prod_bom_vers as b
        on a."Production BOM No_" = b."Production BOM No_" and coalesce(nullif(a."Version Code", ''), '_null') = coalesce(nullif(b."Version Code", ''), '_null')

        where a."Type" = 1

        union all

        select
            c."Production BOM No_",
            c."Version Code",
            c."Location Code",
            a."No_",
            a."Routing Link Code"
        from nav.tbl_int_prod_bom_line as a

        left join nav.tbl_int_prod_bom_vers as b
        on a."Production BOM No_" = b."Production BOM No_" and coalesce(nullif(a."Version Code", ''), '_null') = coalesce(nullif(b."Version Code", ''), '_null')

        inner join bom as c
        on c."No_" = a."Production BOM No_" and (coalesce(nullif(c."Location Code", ''), '_null') = coalesce(nullif(b."Location Code", ''), '_null') or nullif(b."Location Code", '') is null)

        where a."Type" = 2
    )
    select
        a."Production BOM No_",
        a."Version Code",
        a."Location Code",
        a."No_",
        a."Routing Link Code"
    from bom as a

    inner join (
        select distinct
            b1."Item No_"
        from tbl_tmp_stoc as b1
        where b1."Whse_ Document Type" != 5 and b1."Bin Code" = _wrkcentr_compart
    ) as b
    on a."No_" = b."Item No_"

    where (exists (select *  from json_to_recordset(_locations) as x(loc text, compart text) where a."Location Code" = x.loc) or nullif(a."Location Code", '') is null) and nullif(a."Routing Link Code", '') is not null
    
    order by a."Location Code" desc;

    create index if not exists tbl_tmp_bom_ix1 on tbl_tmp_bom ("No_", "Routing Link Code", "Location Code");
    create index if not exists tbl_tmp_bom_ix2 on tbl_tmp_bom ("Production BOM No_", "Location Code");

    /* routing table */
    /************************************/
    create temp table tbl_tmp_routing on commit drop as
    select
        a."Routing No_",
        a."Version Code",
        a."Operation No_",
        a."Next Operation No_",
        a."Work Center No_",
        a."Routing Link Code",
        b."Location Code"
    from nav.tbl_int_routing_line as a

    left join nav.tbl_int_routing_vers as b
    on a."Routing No_" = b."Routing No_" and coalesce(nullif(a."Version Code", ''), '_null') = coalesce(nullif(b."Version Code", ''), '_null');

    create index if not exists tbl_tmp_routing_ix on tbl_tmp_routing ("Routing No_", "Location Code", "Routing Link Code");

    /* result table */
    /************************************/
    create temp table tbl_tmp_rezultat as
    select
        a."Location Code",
        a."Bin Code",
        a."Item No_",
        a."Description",
        a."Inventory Posting Group",
        coalesce(g."Range", a."Range") as "Range",
        a."Last Doc_ Type Name",
        a."Last Source Doc_ Name",
        a."Source No_",
        f."Description" as "Source Description",
        a."Source Line No_",
        /*coalesce(coalesce(coalesce(b."Work Center No_", c."Work Center No_"), d."Work Center No_"), e."Work Center No_") as "Work Center No_",*/
        b."Work Center No_" as "Wrk_ Cntr_ 1",
        c."Work Center No_" as "Wrk_ Cntr_ 2",
        d."Work Center No_" as "Wrk_ Cntr_ 3",
        e."Work Center No_" as "Wrk_ Cntr_ 4",
        a."Unit of Measure Code",
        a."Quantity",
        a."Unit Cost",
        a."Total Cost"
    from tbl_tmp_stoc as a

    left join lateral (
        select distinct on (b1."Prod_ Order No_", b1."Item No_")
            b2."Work Center No_"
        from nav.tbl_int_prod_order_component as b1

        left join nav.tbl_int_prod_order_routing_line as b2
        on b1."Prod_ Order No_" = b2."Prod_ Order No_" and b1."Location Code" = b2."Location Code" and b1."Prod_ Order Line No_" = b2."Routing Reference No_" and b1."Routing Link Code" = b2."Routing Link Code"

        where a."Whse_ Document Type" = 5 and a."Bin Code" = _wrkcentr_compart and b1."Prod_ Order No_" = a."Source No_" and
            exists (select * from json_to_recordset(_locations) as x(loc text, compart text) where b1."Location Code" = x.loc) and
            b1."Item No_" = a."Item No_" and b1."Prod_ Order Line No_" <= a."Source Line No_"

        order by b1."Prod_ Order No_" asc, b1."Item No_" asc, b1."Prod_ Order Line No_" desc
    ) as b
    on true

    left join lateral (
        select distinct on (c1."Prod_ Order No_", c1."Item No_")
            c2."Work Center No_"
        from nav.tbl_int_prod_order_line as c1

        left join nav.tbl_int_prod_order_routing_line as c2
        on c1."Prod_ Order No_" = c2."Prod_ Order No_" and c1."Line No_" = c2."Routing Reference No_"

        where a."Whse_ Document Type" = 5 and a."Bin Code" = _wrkcentr_compart and c1."Prod_ Order No_" = a."Source No_" and c1."Item No_" = a."Item No_"

        order by c1."Prod_ Order No_" asc, c1."Item No_" asc,
            (case when nullif(c2."Next Operation No_", '') is null then 0 else 1 end) asc,
            c2."Operation No_" desc
    ) as c
    on true

    left join lateral (
        select distinct on (d1."No_")
            d3."Work Center No_"
        from tbl_tmp_bom as d1

        inner join nav.tbl_int_item as d2
        on d1."Production BOM No_" = coalesce(nullif(d2."Production BOM No_", ''), '_null')

        inner join tbl_tmp_routing as d3
        on coalesce(nullif(d2."Routing No_", ''), '_null') = d3."Routing No_"

        where a."Whse_ Document Type" != 5 and a."Bin Code" = _wrkcentr_compart and a."Item No_" = d1."No_" and
            coalesce(nullif(d1."Location Code", ''), '_null') = coalesce(nullif(d3."Location Code", ''), '_null') and
            d1."Routing Link Code" = d3."Routing Link Code"

        order by d1."No_" asc, d1."Production BOM No_" asc,
             (case when exists (select * from json_to_recordset(_locations) as x(loc text, compart text) where d3."Location Code" = x.loc) then 0
                    when nullif(d3."Location Code", '') is null then 1
            else 2 end) asc
    ) as d
    on true

    left join lateral (
        select distinct on (e1."No_")
            e2."Work Center No_"
        from nav.tbl_int_item as e1

        inner join tbl_tmp_routing as e2
        on coalesce(nullif(e1."Routing No_", ''), '_null') = e2."Routing No_"

        where a."Whse_ Document Type" != 5 and a."Bin Code" = _wrkcentr_compart and a."Item No_" = e1."No_" and
            (exists (select * from json_to_recordset(_locations) as x(loc text, compart text) where e2."Location Code" = x.loc) or nullif(e2."Location Code", '') is null)

        order by e1."No_" asc, e2."Location Code" desc,
            (case when nullif(e2."Next Operation No_", '') is null then 0 else 1 end) asc,
            e2."Operation No_" desc
    ) as e
    on true

    left join nav.tbl_int_production_order as f
    on coalesce(nullif(a."Source No_", ''), '_null') = f."No_"

    left join nav.tbl_int_item as g
    on f."Source No_" = g."No_";
end;
$$ language plpgsql;

select * from tbl_tmp_rezultat;