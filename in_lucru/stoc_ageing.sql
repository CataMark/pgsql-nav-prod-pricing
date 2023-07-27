do $$
declare
    _counter int := 0;
    _start_date date := '2021-04-30'::date;
    _month_intv int = (date_part('year', current_date) - date_part('year', _start_date)) * 12 + date_part('month', current_date) - date_part('month', _start_date);
begin
    drop table if exists tbl_tmp_rulaj;
    create temp table tbl_tmp_rulaj as
    select
        date_trunc('month', case when a."Posting Date"::date <= _start_date then _start_date else a."Posting Date"::date end)::date as "Posting Date",
        a."Item No_",
        c."Description" as "Item Name",
        c."Inventory Posting Group",
        c."Base Unit of Measure",
        round(sum(a."Quantity"), 2) as "Quantity",
        round(sum(b."Cost"), 2) as "Cost"
    from nav.tbl_int_item_ledger_entry as a

    left join lateral(
        select
            round(sum(b1."Cost Amount (Actual)"), 2) as "Cost"
        from nav.tbl_int_value_entry as b1
        where b1."Item Ledger Entry No_" = a."Entry No_"
    ) as b
    on true

    inner join nav.tbl_int_item as c
    on a."Item No_" = c."No_"

    where c."Inventory Posting Group" in ('AMBALAJE', 'MARF TERT', 'MARFURI', 'MAT FINIS', 'MAT_PRIMA', 'MP_LEMN', 'SEMIFABR')

    group by date_trunc('month', case when a."Posting Date"::date <= _start_date then _start_date else a."Posting Date"::date end)::date,
            a."Item No_",
            c."Description",
            c."Inventory Posting Group",
            c."Base Unit of Measure";
    create unique index if not exists tbl_tmp_rulaj_ix1 on tbl_tmp_rulaj ("Posting Date", "Item No_");

    delete from tbl_tmp_rulaj as a
    using (
        select
            b1."Item No_",
            sum(b1."Quantity") as "Quantity"
        from tbl_tmp_rulaj as b1
        group by b1."Item No_"
    ) as b
    where a."Item No_" = b."Item No_" and b."Quantity" <= 0;

    drop table if exists tbl_tmp_intermed;
    create temp table if not exists tbl_tmp_intermed (like tbl_tmp_rulaj excluding all) on commit drop;
    create index if not exists tbl_tmp_intermed_ix1 on tbl_tmp_intermed ("Posting Date", "Item No_");

    loop
        insert into tbl_tmp_intermed
        select
            (case when a."Quantity" <= 0 and a."Prev Qty" is not null then coalesce(a."Prev Date", a."Curr Date")
                when a."Quantity" <= 0 and a."Prev Qty" is null then coalesce(a."Next Date", a."Curr Date")
                else a."Curr Date"
            end) as "Posting Date",
            a."Item No_",
            a."Item Name",
            a."Inventory Posting Group",
            a."Base Unit of Measure",
            a."Quantity",
            a."Cost"
        from (
            select
                a1."Posting Date" as "Curr Date",
                lag(a1."Posting Date") over (partition by a1."Item No_" order by a1."Posting Date" asc) as "Prev Date",
                lead(a1."Posting Date") over (partition by a1."Item No_" order by a1."Posting Date" asc) as "Next Date",
                a1."Item No_",
                a1."Item Name",
                a1."Inventory Posting Group",
                a1."Base Unit of Measure",
                a1."Quantity",
                lag(a1."Quantity") over (partition by a1."Item No_" order by a1."Posting Date" asc) as "Prev Qty",
                a1."Cost"
            from tbl_tmp_rulaj as a1
            where a1."Quantity" != 0
        ) as a;

        truncate table tbl_tmp_rulaj;
        insert into tbl_tmp_rulaj
        select
            a."Posting Date",
            a."Item No_",
            a."Item Name",
            a."Inventory Posting Group",
            a."Base Unit of Measure",
            sum(a."Quantity") as "Quantity",
            sum(a."Cost") as "Cost"
        from tbl_tmp_intermed as a
        group by 
            a."Posting Date",
            a."Item No_",
            a."Item Name",
            a."Inventory Posting Group",
            a."Base Unit of Measure";
        
        truncate table tbl_tmp_intermed;

        _counter := _counter + 1;

        exit when not exists (select * from tbl_tmp_rulaj where "Quantity" <= 0) or _counter > _month_intv;
    end loop;

    raise notice 'Looped % time(s)', _counter;
end;
$$ language plpgsql;

select * from tbl_tmp_rulaj as a
order by a."Item No_" asc, a."Posting Date" asc;