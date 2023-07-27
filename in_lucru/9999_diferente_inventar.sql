do $$
begin
    drop table if exists tbl_tmp_diff;
    drop table if exists tbl_tmp_comp_rezl;
    
    create temp table tbl_tmp_diff(
        reper text not null,
        nume text,
        diff numeric(10, 5) not null,
        umas text,
        alocat text,
        constraint tbl_tmp_diff_pk primary key (reper)
    );

    insert into tbl_tmp_diff(reper, nume, diff, umas, alocat)
    values ('AMPAL0004','PALET 1200X800X150 UNI',-502.43,'UN','NULL'),
            ('TARFA0007','TARG',-1292,'UN','NULL'),
            ('PICST0056','PICIOR STG SPATE',-297,'UN','NULL'),
            ('AMPAL0006','PALET 2000X800X150 UNI',-155,'UN','NULL'),
            ('6012CO12C','PERMA BLUE - COLOANA L120 2S - COLET 1/1',-26,'UN','NULL'),
            ('H171006T','PICIOR MASA',-114,'UN','NULL'),
            ('PSUMD0015','PLACA SUPERIOARA',-268,'UN','NULL'),
            ('MONMD0007','MONTANT DREAPTA',-381,'UN','NULL'),
            ('6009ARMOC','PERMA ALB - DULAPIOR OGLINDA 1U - COLET 1/1',-32,'UN','NULL'),
            ('ADAST0004','ADAOS',-4227,'UN','NULL'),
            ('6012DS60C','PERMA BLUE - BLAT L60 - COLET 1/1',-45,'UN','NULL'),
            ('FIGRU0100','GRUND DE ADERENTA PENTRU REP. UVF5782',-55,'KG','NULL'),
            ('6012CO18C','PERMA BLUE - COLOANA L185 1U - COLET 1/1',-12,'UN','NULL'),
            ('MACSU0006','CHER STEJAR USCAT 50 MM',-2.152,'M3','NULL'),
            ('AMPAL0005','PALET 2000X1000X150 UNI',-59.816,'UN','NULL'),
            ('FSEMD0015','FATA SERTAR INFERIOR GRI',-94,'UN','NULL'),
            ('FILAC0088','LAC FARMHOUSE placa WM1607-0010',-80,'KG','NULL'),
            ('MONMD0012','MONTANT STANGA',-160,'UN','NULL'),
            ('3628TA2Z','HANA - MASA - COLET 2/2',-5,'UN','NULL'),
            ('3534L61Z','ADAMS - PAT L260 - COLET 1/4',-10,'UN','NULL'),
            ('FIGRU0092','FOND RULOU 4250-777001-206',-96.107,'L','NULL'),
            ('H441009A','USA',-30,'UN','NULL'),
            ('MADSUC002','SEMIF STEJAR USCAT 34 MM',-1.278,'M3','NULL'),
            ('FILAC0089','LAC FARMHOUSE targ WHB667-90033',-80,'KG','NULL'),
            ('SIBST0008','SIPCA SUPORT SERTAR MIC',-690,'UN','NULL'),
            ('PAFOL0039','LATERALA DR SERTAR 0375.0X0175.0X012.0/F0',-588,'UN','NULL'),
            ('PUNFE0883','PUNGA FERONERIE 6008SV60C AVELA',-162,'UN','NULL'),
            ('AMFOL0008','FOLIE NEPERFORATA  LATIME 2.2 M-LUNGIME 2.2M',-169.562,'KG','NULL'),
            ('PANHD0015','PANOU TAVA',-301,'UN','NULL'),
            ('D561003T','FATA SERTAR MARE',-93,'UN','NULL'),
            ('FIGRU0097','FOND APA UV UVA5333/N65202 ICA',-49.001,'KG','NULL'),
            ('TRAST0066','TRAVERSA SUP PER LAT',-210,'UN','NULL'),
            ('AMPAL0009','PALET 2000X900X150 UNI pentru ANAVIL',-17,'UN','NULL'),
            ('FIGRU0075','FOND ROSU MARYD RULOU UVS5806/N62241',-25,'KG','NULL'),
            ('DSFCO0717ST','PLACA SUPERIOARA',-13,'UN','NULL'),
            ('PAFCO0442','PANOU CAPAT MARE',-17,'UN','NULL'),
            ('PICMD0025','PICIOR CAPAT MARE DR',-93,'UN','NULL'),
            ('SPSPA0029','SPATE SERTAR NOPTIERA',-501,'UN','NULL'),
            ('CORMD0001','CORNISA LATERALA',-131,'UN','NULL'),
            ('PALET0067','PALET CARTON TIP TRAY 452 LB10 - 2338X1004X130',-26.164,'UN','NULL'),
            ('GEOGL0048','OGLINDA 0680X480X3 CU FOLIE',-39,'UN','NULL'),
            ('FICAT0006','FOTOFINITIATOR PT. FOND ICA FI59',-5.5,'KG','NULL'),
            ('HDSPA0002','SPATE GRI 0682.0X0482.0X002.5/F0',-186,'UN','NULL'),
            ('FIGRU0057','FOND FARMFOUSE targ ED1228-9001 (ED/93001-166)',-79,'KG','NULL'),
            ('LEGPA0066','LEGATURA SPATE ALBASTRU',-228,'UN','NULL'),
            ('FILAC0090','LAC FARMHOUSE picior EMK223-0030',-75,'L','NULL'),
            ('MADSUC008','SEMIF STEJAR USCAT 27 MM',-0.506461800000011,'M3','NULL'),
            ('FIGRU0095','GRUND2 RULOU KLIMPFJALL IKEA UL1300-0001BF',-23,'KG','NULL'),
            ('0199TA151','ADELITA - MASA 1500/2300/900 GCJ877 COLET 1/2',-2,'UN','NULL'),
            ('PANPA0240','PANOU PROTECTIE 1400X1000X16',-40,'UN','NULL'),
            ('6012DS80C','PERMA BLUE - BLAT L80 - COLET 1/1',-9,'UN','NULL'),
            ('3024A3PT1','CLEMENCE GRI - DULAP 3 USI 3 SERTARE - COLET 1/4',-2,'UN','NULL'),
            ('6008FB60C','AVELA - CADRU PICIOR L60 - COLET 1/1',-9,'UN','NULL'),
            ('SOCMD0024','SOCLU LATERAL',-176,'UN','NULL'),
            ('D861002T','PANOU CAPAT MIC+ETICHETA LOT',-17,'UN','NULL'),
            ('0199BANCC','ADELITA - BANCA GAU351 - COLET 1/1',-2,'UN','NULL'),
            ('PAFOL0083','SPATE SERTAR 0350.0X0078.0X012.0/F0',-507,'UN','NULL'),
            ('ADAFA0014','ADAOS GLISIERA PERETE LATERAL',-394,'UN','NULL'),
            ('PANPA0361','PANOU PROTECTIE 1000X700X16',-38.73,'UN','NULL'),
            ('AMMAN0016','MANSON 0200.0X0810.0X0200.0X770.0/F0CO5',-150,'UN','NULL'),
            ('COPIC0009','PICIOR MESTEACAN',-30,'UN','NULL'),
            ('FILAC0105','LAC CANT APA MONO ROSU AO850G20/N62241',-13,'KG','NULL'),
            ('PANPA0005','PANOU PROTECTIE 1100X1000X16',-35,'UN','NULL'),
            ('AMOCO0003','ANSAMBLU PLACA MASA',-5,'UN','NULL'),
            ('6010SV80C','PERMA GRI - MASCA CHIUVETA L80 2S - COLET 1/1',-1,'UN','NULL'),
            ('FILAC0101','LAC GRI RUL UVO5095G10/N61553',-6,'KG','NULL'),
            ('ADEZI0043','ADEZIV PENTRU CEPURI JOWACOLLl 114.60',-25,'KG','NULL'),
            ('PANPA0363','PANOU PROTECTIE 800X700X16',-32,'UN','NULL'),
            ('FILAC0111','LAC LUCIOS VERDE UVO5640G80N48250',-9.9,'KG','NULL'),
            ('PSUST0001','PLACA SUPERIORA',-5,'UN','NULL'),
            ('FUHCA0009','HIRTIE BANDA CANT ALBA 22MM',-3000,'ML','NULL'),
            ('FILAC0073','LAC RU UM1135-0012 SW',-5.002,'L','NULL'),
            ('TRAST0091','TRAVERSA SUPERIOARA PERETE LATERAL',-60,'UN','NULL'),
            ('POFPA0387','POLITA FIXA SUPERIOARA',-22,'UN','NULL'),
            ('AMCAR0005','CARTON TIP III DIM:2000*1000 B.111 FSC',-159.26,'M2','NULL'),
            ('FIGRU0101','FOND ALBASTRU PERMA PULV. UVF5342/N61153',-10.003,'KG','NULL'),
            ('6009SV1PC','PERMA ALB - MASCA CHIUVETA 1 USA - COLET 1/1',-1,'UN','NULL'),
            ('AMCUT0383','CUTIE PLIC 1020.0X0680.0X080.0/F0CO5-IKEA LB05',-31,'UN','NULL'),
            ('PANPA0249','PANOU PROTECTIE 2000X800X16',-1.1,'UN','NULL'),
            ('TRAST0076','TRAVERSA USA LATERALA',-31,'UN','NULL'),
            ('FSEMD0020','FATA SERTAR SUPERIOR',-8,'UN','NULL'),
            ('0162BIBL2','COTTAGE - BIBLIOTECA - COLET 2/2',-1,'UN','NULL'),
            ('FUHCA0015','HIRTIE BANDA CANT(MEL NELACUITA) GRI 20/0,3MM',-1226.134,'ML','NULL'),
            ('PUNFE0889','PUNGA FERONERIE 6008FB80C / 6008FB60CAVELA',-50,'UN','NULL'),
            ('FILAC0093','LAC LARSFRID APA MONO AO1005G10/N48347',-5.738,'KG','NULL'),
            ('PANPA8888','PANOU PROTECTIE 900X700X16',-12,'UN','NULL'),
            ('PINPA0148','PLACA INFERIOARA',-5,'UN','NULL'),
            ('ADHAR0006','ADEZIV TRANSPARENT SOUDAL FIX',-5.766,'UN','NULL'),
            ('PSUST0011','PLACA ALLO',-4,'UN','NULL'),
            ('PANPA0236','PANOU PROTECTIE 800X200X16',-78.74,'UN','NULL'),
            ('LSEPA9001','LATERALA SERTAR',-14,'UN','NULL'),
            ('PANMD0004','CAPAT MIC PAT',-2,'UN','NULL'),
            ('SIPST8007','SIPCA SUPORT SERTAR MIC STEJ',-57,'UN','NULL'),
            ('FECEP0819','CEP ROTUND 8X30 BIRCH/BEECH FSC 100%',-8969,'UN','NULL'),
            ('CARTP0036','PICIOR 90X90X50 PT PALETI IKEA',-223,'UN','NULL'),
            ('SIPFA0001','SIPCA PRINDERE BLAT',-34,'UN','NULL'),
            ('6010DS60C','PERMA GRI - BLAT L60 - COLET 1/1',-1,'UN','NULL'),
            ('D561010T','POLITA MOBILA SERTAR MIC',-4,'UN','NULL');

    /*create temp table tbl_tmp_comp_rezl as
    with vals as (
        select
            b."Prod_ Order No_",
            b."Prod_ Order Line No_",
            b."Line No_",
            b."Item No_",
            b."Description",
            b."Unit of Measure Code",
            (case d."Status" when 4 then 'Da' else 'Nu' end) as "Finished Order",
            nullif(d."Finished Date"::date, '1753-01-01'::date) as "Finished Date",
            (b."Quantity" - c."Finished Quantity" * b."Quantity per") as distrib_qty
        from tbl_tmp_diff as a

        inner join nav.tbl_int_prod_order_component as b
        on a.reper = b."Item No_" and a.umas = b."Unit of Measure Code"

        inner join nav.tbl_int_prod_order_line as c
        on b."Prod_ Order No_" = c."Prod_ Order No_" and b."Prod_ Order Line No_" = c."Line No_"

        inner join nav.tbl_int_production_order as d
        on c."Prod_ Order No_" = d."No_"*/

        /*left join lateral (
            select
                sum(e1."Quantity") as "Quantity"
            from nav.tbl_int_item_ledger_entry as e1
            where e1."Order No_" = b."Prod_ Order No_" and e1."Order Line No_" = b."Prod_ Order Line No_" and e1."Item No_" = b."Item No_" and e1."Entry Type" = 5
        ) as e
        on true */

        /*where (d."Status" = 3 or (d."Status" = 4 and d."Finished Date"::date between '2022-04-01'::date and '2022-04-30'::date)) and c."Location Code" = 'AIS' and
            (b."Quantity" - c."Finished Quantity" * b."Quantity per") < 0
    )
    select
        c."Prod_ Order No_",
        c."Prod_ Order Line No_",
        c."Line No_",
        c."Item No_",
        c."Description",
        c."Unit of Measure Code",
        c."Finished Order",
        c."Finished Date",
        a.alocat,
        (case when b.total_qty >= a.diff then -1 * c.distrib_qty
            else -1 * c.distrib_qty/ b.total_qty * a.diff end) as distrib_qty
    from tbl_tmp_diff as a

    inner join (
        select
            b1."Item No_",
            sum(b1.distrib_qty) as total_qty
        from vals as b1
        group by b1."Item No_"
    ) as b
    on a.reper = b."Item No_"

    inner join vals as c
    on a.reper = c."Item No_";*/

    create temp table tbl_tmp_comp_rezl as
    with vals as (
        select
            b."Prod_ Order No_",
            b."Prod_ Order Line No_",
            c."Item No_" as "Source No_",
            b."Line No_",
            b."Item No_",
            b."Description",
            b."Unit of Measure Code",
            (case d."Status" when 4 then 'Da' else 'Nu' end) as "Finished Order",
            nullif(d."Finished Date"::date, '1753-01-01'::date) as "Finished Date",
            (c."Finished Quantity" * coalesce(nullif(b."Quantity per", 0), 0.1)) as distrib_qty
        from tbl_tmp_diff as a

        inner join nav.tbl_int_prod_order_component as b
        on a.reper = b."Item No_" and a.umas = b."Unit of Measure Code"

        inner join nav.tbl_int_prod_order_line as c
        on b."Prod_ Order No_" = c."Prod_ Order No_" and b."Prod_ Order Line No_" = c."Line No_"

        inner join nav.tbl_int_production_order as d
        on c."Prod_ Order No_" = d."No_"

        where (d."Status" = 3 or (d."Status" = 4 and d."Finished Date"::date between '2022-04-01'::date and '2022-04-30'::date)) and c."Location Code" = 'AIS' and
            c."Finished Quantity" > 0
    )
    select
        c."Prod_ Order No_",
        c."Prod_ Order Line No_",
        c."Source No_",
        c."Line No_",
        c."Item No_",
        c."Description",
        c."Unit of Measure Code",
        c."Finished Order",
        c."Finished Date",
        a.alocat,
        round(-1 * c.distrib_qty/ coalesce(nullif(b.total_qty, 0), 1) * a.diff, 5) as distrib_qty
    from tbl_tmp_diff as a

    inner join (
        select
            b1."Item No_",
            sum(b1.distrib_qty) as total_qty
        from vals as b1
        group by b1."Item No_"
    ) as b
    on a.reper = b."Item No_"

    inner join vals as c
    on a.reper = c."Item No_";
end;
$$ language plpgsql;

select * from tbl_tmp_comp_rezl;