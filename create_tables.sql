/* code block start */
do $$
declare
    _dbname text := current_database();
begin
    if _dbname != 'any' then
        raise exception 'not in "any" database';
    end if;

/* 0000.01 */
raise notice 'creating schema "ambi';
create schema if not exists ambi;
raise notice '************************************';

/* 0001.01 */
raise notice 'creating table "tbl_int_cifraj_unit_cost"';
create table if not exists ambi.tbl_int_cifraj_unit_cost(
    item_no text not null,
    descr text not null,
    umas text not null,
    cost decimal(38, 20) not null,
    data_estim date not null,
    mod_de timestamp constraint tbl_int_cifraj_unit_cost_df_ts default current_timestamp not null,
    constraint tbl_int_cifraj_unit_cost_pk primary key (item_no)
);
raise notice '************************************';

/* 0001.02 */
raise notice 'creating table "tbl_int_cifraj_unit_cost_load"';
create table if not exists ambi.tbl_int_cifraj_unit_cost_load(
    item_no text not null,
    descr text not null,
    umas text not null,
    cost decimal(38, 20) not null,
    data_estim date not null,
    mod_de timestamp constraint tbl_int_cifraj_unit_cost_load_df_ts default current_timestamp not null,
    constraint tbl_int_cifraj_unit_cost_load_pk primary key (item_no)
);
raise notice '************************************';

/* 0001.03 */
raise notice 'creating table "tbl_int_fx_rate"';
create table if not exists ambi.tbl_int_fx_rate(
    currency text not null,
    valoare decimal(10,5) not null,
    data_val date not null,
    constraint tbl_int_fx_rate_pk primary key (currency)
);
raise notice '************************************';

/* 0002.01 */
raise notice 'creating table "tbl_int_cifraj_unit_cost_archv"';
create table if not exists ambi.tbl_int_cifraj_unit_cost_archv(
    "BOM Date" date,
    item_no text,
    descr text,
    umas text,
    cost numeric(38,20),
    data_estim date,
    mod_de timestamp
);
create unique index if not exists tbl_int_cifraj_unit_cost_archv_ix1 on ambi.tbl_int_cifraj_unit_cost_archv(item_no, "BOM Date");
raise notice '************************************';

/* 0002.02 */
raise notice 'creating table "tbl_int_fx_rate_archv"';
create table if not exists ambi.tbl_int_fx_rate_archv(
    "BOM Date" date,
    currency text,
    valoare numeric(10,5),
    data_val date
);
create unique index if not exists tbl_int_fx_rate_archv_ix1 on ambi.tbl_int_fx_rate_archv(currency, "BOM Date");
raise notice '************************************';

/* 0002.03 */
raise notice 'creating table "tbl_mdl_prod_bom_colete_archv"';
create table if not exists ambi.tbl_mdl_prod_bom_colete_archv(
    "BOM Date" date,
    "Item No_" text,
    "Description" text,
    "Range" text,
    "Item Type" text,
    "Cod ABCD" text,
    "Gross Weight" numeric(38,20),
    "Net Weight" numeric(38,20),
    "Length" numeric,
    "Width" numeric,
    "Height" numeric,
    "Unit Volume" numeric(38,20),
    "Unit of Measure" text,
    "Last sales price" numeric,
    "Price Includes VAT" boolean,
    "NAV Unit Cost" numeric(38,20),
    "NAV Standard Cost" numeric(38,20),
    "NAV Last Direct Cost" numeric(38,20),
    "BOM No_" text,
    "BOM Version" text,
    "BOM Location" text,
    "Factory" text,
    "Routing No_" text,
    "Route Version" text,
    "Route Location" text,
    "Avg Qty Type" text,
    "Avg Quantity" numeric
);
create unique index if not exists tbl_mdl_prod_bom_colete_archv_ix1 on ambi.tbl_mdl_prod_bom_colete_archv("Item No_", "BOM Date");

if not exists (select * from information_schema.columns
            where table_catalog = 'any' and table_schema = 'ambi' and
            table_name = 'tbl_mdl_prod_bom_colete_archv' and column_name = 'First Prod_ Date') then
    alter table ambi.tbl_mdl_prod_bom_colete_archv add column "First Prod_ Date" date;
end if;

if not exists (select * from information_schema.columns
            where table_catalog = 'any' and table_schema = 'ambi' and
            table_name = 'tbl_mdl_prod_bom_colete_archv' and column_name = 'First Sale Date') then
    alter table ambi.tbl_mdl_prod_bom_colete_archv add column "First Sale Date" date;
end if;
raise notice '************************************';

/* 0002.04 */
raise notice 'creating table "tbl_mdl_bom_comp_expanded_archv"';
create table if not exists ambi.tbl_mdl_bom_comp_expanded_archv(
    "BOM Date" date,
    "Colet Item No_" text,
    "Colet Description" text,
    "Range" text,
    "Colet Avg Qty Type" text,
    "Colet Avg Qty" numeric,
    "Level" integer,
    "Line No_" text,
    "Line Type" integer,
    "Is Leaf" boolean,
    "Item No_" text,
    "Item Description" text,
    "Factory" text,
    "BOM No_" text,
    "BOM Version" text,
    "BOM Location" text,
    "Routing No_" text,
    "Route Version" text,
    "Route Location" text,
    "Routing Link Code" text,
    "Item Type" text,
    "Item Family" text,
    "Quantity per" numeric(38,20),
    "Quantity" numeric(38,20),
    "Scrap _" numeric(38,20),
    "Parent Route Fixed Scrap Qty" numeric(38,20),
    "Parent Route Scrap Factor" numeric(38,20),
    "Parent Route Fixed Scrap Qty Accum" numeric(38,20),
    "Parent Route Scrap Factor Accum" numeric(38,20),
    "Unit of Measure Code" text,
    "Quantity w Scrap" numeric(38,20),
    "NAV Unit Cost" numeric(15,5),
    "NAV Standard Cost" numeric(15,5),
    "NAV Last Direct Cost" numeric(15,5),
    "Cifraj Unit Cost" numeric(15,5),
    "Last purchase price" numeric(15,5),
    "Legacy Unit Cost" numeric(15,5),
    "Unit Cost Type" text,
    "Unit Cost" numeric(15,5),
    "UMAS Convert Errors" text
);
create index if not exists tbl_mdl_bom_comp_expanded_archv_ix1 on ambi.tbl_mdl_bom_comp_expanded_archv("Colet Item No_", "BOM Date");
raise notice '************************************';

/* 0002.05 */
raise notice 'creating table "tbl_mdl_bom_route_expanded_archv"';
create table if not exists ambi.tbl_mdl_bom_route_expanded_archv(
    "BOM Date" date,
    "Colet Item No_" text,
    "Colet Description" text,
    "Colet Avg Qty Type" text,
    "Colet Avg Qty" numeric,
    "Level" integer,
    "Line No_" text,
    "Item No_" text,
    "Item Description" text,
    "Item Type" text,
    "Factory" text,
    "BOM No_" text,
    "BOM Version" text,
    "BOM Location" text,
    "Quantity w Scrap" numeric(38,20),
    "Unit of Measure Code" text,
    "Routing No_" text,
    "Route Version" text,
    "Route Location" text,
    "Operation No_" text,
    "Next Operation No_" text,
    "Previous Operation No_" text,
    "Type" integer,
    "No_" text,
    "Description" text,
    "Work Center No_" text,
    "Work Center Group Code" text,
    "Setup Time" numeric(38,20),
    "Setup Time Unit of Meas_ Code" text,
    "Run Time" numeric(38,20),
    "Run Time Unit of Meas_ Code" text,
    "Routing Link Code" text,
    "Machine Efficiency (_)" integer,
    "No_ Of Workers" integer,
    "Machine Time" numeric,
    "Workers Time" numeric
);
create index if not exists tbl_mdl_bom_route_expanded_archv_ix1 on ambi.tbl_mdl_bom_route_expanded_archv("Colet Item No_", "BOM Date");
raise notice '************************************';

/* 0002.06 */
raise notice 'creating table "tbl_mdl_kit_comp_other_archv"';
create table if not exists ambi.tbl_mdl_kit_comp_other_archv(
    "BOM Date" date,
    "Kit No_" text,
    "Kit Description" text,
    "Kit Range" text,
    "Cod ABCD" text,
    "Item No_" text,
    "Item Description" text,
    "Item Type" text,
    "Factory" text,
    "Item Family" text,
    "Quantity per" numeric(38,20),
    "Unit of Measure Code" text,
    "Gross Weight" numeric(38,20),
    "Net Weight" numeric(38,20),
    "Length" numeric,
    "Width" numeric,
    "Height" numeric,
    "Unit Volume" numeric(38,20),
    "Unit Cost Type" text,
    "Unit Cost" numeric,
    "UMAS Convert Errors" text
);
create index if not exists tbl_mdl_kit_comp_other_archv_ix1 on ambi.tbl_mdl_kit_comp_other_archv("Kit No_", "BOM Date");

if not exists (select * from information_schema.columns
            where table_catalog = 'any' and table_schema = 'ambi' and
            table_name = 'tbl_mdl_kit_comp_other_archv' and column_name = 'Kit First Prod_ Date') then
    alter table ambi.tbl_mdl_kit_comp_other_archv add column "Kit First Prod_ Date" date;
end if;

if not exists (select * from information_schema.columns
            where table_catalog = 'any' and table_schema = 'ambi' and
            table_name = 'tbl_mdl_kit_comp_other_archv' and column_name = 'Kit First Sale Date') then
    alter table ambi.tbl_mdl_kit_comp_other_archv add column "Kit First Sale Date" date;
end if;

if not exists (select * from information_schema.columns
            where table_catalog = 'any' and table_schema = 'ambi' and
            table_name = 'tbl_mdl_kit_comp_other_archv' and column_name = 'Item First Prod_ Date') then
    alter table ambi.tbl_mdl_kit_comp_other_archv add column "Item First Prod_ Date" date;
end if;

if not exists (select * from information_schema.columns
            where table_catalog = 'any' and table_schema = 'ambi' and
            table_name = 'tbl_mdl_kit_comp_other_archv' and column_name = 'Item First Sale Date') then
    alter table ambi.tbl_mdl_kit_comp_other_archv add column "Item First Sale Date" date;
end if;
raise notice '************************************';

/* 0003.01 */
raise notice 'creating table "tbl_mdl_gl_entry_reconciliation"';
create table if not exists ambi.tbl_mdl_gl_entry_reconciliation(
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
);
create unique index if not exists tbl_mdl_gl_entry_reconciliation_ix1 on ambi.tbl_mdl_gl_entry_reconciliation ("Transaction No_", "Entry No_", "Bal_ Entry No_");
create index if not exists tbl_mdl_gl_entry_reconciliation_ix2 on ambi.tbl_mdl_gl_entry_reconciliation ("Posting Date", "Transaction No_", "Entry No_");
raise notice '************************************';

/* code block ending*/
end;
$$ language plpgsql;