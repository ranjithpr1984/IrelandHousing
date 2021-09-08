-- Databricks notebook source
-- DBTITLE 1,Create table
CREATE TABLE IF NOT EXISTS property_price_data
  USING DELTA
  LOCATION '/user/hive/warehouse/property_price_data';

-- COMMAND ----------

-- DBTITLE 1,Data count after scrapping
select nvl(website,'Total') Website,count(1) as Count
from default.property_price_data
group by website with ROLLUP

-- COMMAND ----------

-- DBTITLE 1,Sample of duplicate data
select county,price,beds,propertyType,
collect_list(website) as websites,
collect_list(address) as address
from default.property_price_data
group by county,town,price,beds,propertyType,lower(regexp_replace(regexp_extract(address,'^(.+?),'),'[^A-Za-z0-9]',''))
having count(1) > 1 and min(website)!=max(website)

-- COMMAND ----------

-- DBTITLE 1,Duplicate data count
with dup as (
  select county, price, lower(town) as town, beds, propertyType, min(website) as website,
         lower(regexp_replace(regexp_extract(address,'^(.+?),'),'[^A-Za-z0-9]','')) addressPart
    from default.property_price_data
   group by county, price, lower(town), beds, propertyType,
            lower(regexp_replace(regexp_extract(address,'^(.+?),'),'[^A-Za-z0-9]',''))
  having count(1) > 1 and min(website) != max(website)
)
select nvl(website,'Total') Website,count(1) as Count
  from default.property_price_data as pd
 where exists(
   select 'x'from dup 
   where pd.county = dup.county and pd.price = dup.price and lower(pd.town) = dup.town
     and pd.beds = dup.beds and pd.propertyType = dup.propertyType and pd.website != dup.website
     and lower(regexp_replace(regexp_extract(pd.address,'^(.+?),'),'[^A-Za-z0-9]','')) = dup.addressPart
  )
 group by website with ROLLUP

-- COMMAND ----------

-- DBTITLE 1,Duplicate data deletion
with dup as (
  select county, price, lower(town) as town, beds, propertyType, min(website) as website,
         lower(regexp_replace(regexp_extract(address,'^(.+?),'),'[^A-Za-z0-9]','')) addressPart
    from default.property_price_data
   group by county, price, lower(town), beds, propertyType,
            lower(regexp_replace(regexp_extract(address,'^(.+?),'),'[^A-Za-z0-9]',''))
  having count(1) > 1 and min(website) != max(website)
)
delete from default.property_price_data as pd
 where exists(
    select 'x'from dup 
    where pd.county = dup.county and pd.price = dup.price and lower(pd.town) = dup.town
      and pd.beds = dup.beds and pd.propertyType = dup.propertyType and pd.website != dup.website
      and lower(regexp_replace(regexp_extract(pd.address,'^(.+?),'),'[^A-Za-z0-9]','')) = dup.addressPart
  )

-- COMMAND ----------

-- DBTITLE 1,Data count after duplicate deletion
select nvl(website,'Total') Website,count(1) as Count
from default.property_price_data
group by website with ROLLUP

-- COMMAND ----------

create view if not exists dublin_postal_dist as (
select "Dublin 1" postal_dist, "Abbey St" area union all
select "Dublin 1" postal_dist, "Amiens St" area union all
select "Dublin 1" postal_dist, "Capel St" area union all
select "Dublin 1" postal_dist, "Dorset St" area union all
select "Dublin 1" postal_dist, "Henry St" area union all
select "Dublin 1" postal_dist, "Mountjoy Sq" area union all
select "Dublin 1" postal_dist, "Marlboro St" area union all
select "Dublin 1" postal_dist, "North Wall" area union all
select "Dublin 1" postal_dist, "O’Connell St" area union all
select "Dublin 1" postal_dist, "Parnell Sq" area union all
select "Dublin 1" postal_dist, "Talbot St" area union all
select "Dublin 10" postal_dist, "Ballyfermot" area union all
select "Dublin 11" postal_dist, "Ballygal" area union all
select "Dublin 11" postal_dist, "Cappagh" area union all
select "Dublin 11" postal_dist, "Cremore" area union all
select "Dublin 11" postal_dist, "Dubber" area union all
select "Dublin 11" postal_dist, "Finglas" area union all
select "Dublin 11" postal_dist, "Jamestown" area union all
select "Dublin 11" postal_dist, "Kilshane" area union all
select "Dublin 11" postal_dist, "Wadelai" area union all
select "Dublin 11" postal_dist, "The Ward" area union all
select "Dublin 12" postal_dist, "Bluebell" area union all
select "Dublin 12" postal_dist, "Crumlin" area union all
select "Dublin 12" postal_dist, "Drimnagh" area union all
select "Dublin 12" postal_dist, "Walkinstown" area union all
select "Dublin 13" postal_dist, "Baldoyle" area union all
select "Dublin 13" postal_dist, "Bayside" area union all
select "Dublin 13" postal_dist, "Donaghmede" area union all
select "Dublin 13" postal_dist, "Sutton" area union all
select "Dublin 13" postal_dist, "Howth" area union all
select "Dublin 13" postal_dist, "Portmarnock" area union all
select "Dublin 13" postal_dist, "Drumnigh Road" area union all
select "Dublin 14" postal_dist, "Churchtown" area union all
select "Dublin 14" postal_dist, "Dundrum" area union all
select "Dublin 14" postal_dist, "Goatstown" area union all
select "Dublin 14" postal_dist, "Roebuck" area union all
select "Dublin 14" postal_dist, "Windy Arbour" area union all
select "Dublin 14" postal_dist, "Clonskeagh" area union all
select "Dublin 15" postal_dist, "Blanchardstown" area union all
select "Dublin 15" postal_dist, "Castleknock" area union all
select "Dublin 15" postal_dist, "Clonee" area union all
select "Dublin 15" postal_dist, "Clonsilla" area union all
select "Dublin 15" postal_dist, "Corduff" area union all
select "Dublin 15" postal_dist, "Mulhuddart" area union all
select "Dublin 16" postal_dist, "Ballinteer" area union all
select "Dublin 16" postal_dist, "Ballyboden" area union all
select "Dublin 16" postal_dist, "Knocklyon" area union all
select "Dublin 16" postal_dist, "Sandyford" area union all
select "Dublin 17" postal_dist, "Belcamp" area union all
select "Dublin 17" postal_dist, "Balgrifin" area union all
select "Dublin 17" postal_dist, "Clonshaugh" area union all
select "Dublin 17" postal_dist, "Priorswood" area union all
select "Dublin 17" postal_dist, "Darndale" area union all
select "Dublin 17" postal_dist, "Riverside" area union all
select "Dublin 17" postal_dist, "Kinsealy" area union all
select "Dublin 18" postal_dist, "Cabinteely" area union all
select "Dublin 18" postal_dist, "Carrickmines" area union all
select "Dublin 18" postal_dist, "Foxrock" area union all
select "Dublin 18" postal_dist, "Kilternan" area union all
select "Dublin 18" postal_dist, "Sandyford" area union all
select "Dublin 18" postal_dist, "Ticknock" area union all
select "Dublin 18" postal_dist, "Ballyedmonduff" area union all
select "Dublin 18" postal_dist, "Stepaside" area union all
select "Dublin 18" postal_dist, "Leopardstown" area union all
select "Dublin 18" postal_dist, "Shankill" area union all
select "Dublin 18" postal_dist, "Ballybrack" area union all
select "Dublin 2" postal_dist, "Baggot St Upper, and Lower" area union all
select "Dublin 2" postal_dist, "College Green" area union all
select "Dublin 2" postal_dist, "Fitzwilliam Square" area union all
select "Dublin 2" postal_dist, "Harcourt Street" area union all
select "Dublin 2" postal_dist, "Kildare Street" area union all
select "Dublin 2" postal_dist, "Lord Edward Street" area union all
select "Dublin 2" postal_dist, "Merrion Square" area union all
select "Dublin 2" postal_dist, "Mount Street Upper, and Lower" area union all
select "Dublin 2" postal_dist, "Nassau Street" area union all
select "Dublin 2" postal_dist, "Pearse Street" area union all
select "Dublin 2" postal_dist, "St. Stephen’s Green" area union all
select "Dublin 2" postal_dist, "South Great Georges Street" area union all
select "Dublin 2" postal_dist, "Leeson Street Upper and Lower" area union all
select "Dublin 20" postal_dist, "Chapelizod" area union all
select "Dublin 20" postal_dist, "Palmerstown" area union all
select "Dublin 22" postal_dist, "Bawnogue" area union all
select "Dublin 22" postal_dist, "Clondalkin" area union all
select "Dublin 22" postal_dist, "Neilstown" area union all
select "Dublin 22" postal_dist, "Newcastle" area union all
select "Dublin 24" postal_dist, "Firhouse" area union all
select "Dublin 24" postal_dist, "Jobstown" area union all
select "Dublin 24" postal_dist, "Kilnamanagh" area union all
select "Dublin 24" postal_dist, "Oldbawn" area union all
select "Dublin 24" postal_dist, "Tallaght" area union all
select "Dublin 24" postal_dist, "Saggart" area union all
select "Dublin 24" postal_dist, "Brittas" area union all
select "Dublin 24" postal_dist, "Rathcoole" area union all
select "Dublin 24" postal_dist, "Citywest" area union all
select "Dublin 3" postal_dist, "Ballybough" area union all
select "Dublin 3" postal_dist, "Cloniffe" area union all
select "Dublin 3" postal_dist, "Clontarf" area union all
select "Dublin 3" postal_dist, "Dollymount" area union all
select "Dublin 3" postal_dist, "East Wall" area union all
select "Dublin 3" postal_dist, "Fairview" area union all
select "Dublin 3" postal_dist, "Marino" area union all
select "Dublin 4" postal_dist, "Ballsbridge" area union all
select "Dublin 4" postal_dist, "Donnybrook" area union all
select "Dublin 4" postal_dist, "Irishtown" area union all
select "Dublin 4" postal_dist, "Merrion" area union all
select "Dublin 4" postal_dist, "Pembroke" area union all
select "Dublin 4" postal_dist, "Ringsend" area union all
select "Dublin 4" postal_dist, "Sandymount" area union all
select "Dublin 5" postal_dist, "Artane" area union all
select "Dublin 5" postal_dist, "Harmonstown" area union all
select "Dublin 5" postal_dist, "Raheny" area union all
select "Dublin 6" postal_dist, "Dartry" area union all
select "Dublin 6" postal_dist, "Ranelagh" area union all
select "Dublin 6" postal_dist, "Rathmines" area union all
select "Dublin 6" postal_dist, "Rathgar" area union all
select "Dublin 6W" postal_dist, "Harold’s Cross" area union all
select "Dublin 6W" postal_dist, "Templeogue" area union all
select "Dublin 6W" postal_dist, "Terenure" area union all
select "Dublin 7" postal_dist, "Arbour Hill" area union all
select "Dublin 7" postal_dist, "Cabra" area union all
select "Dublin 7" postal_dist, "Phibsborough" area union all
select "Dublin 7" postal_dist, "Four Courts" area union all
select "Dublin 8" postal_dist, "Dolphin’s Barn" area union all
select "Dublin 8" postal_dist, "Inchicore" area union all
select "Dublin 8" postal_dist, "Island Bridge" area union all
select "Dublin 8" postal_dist, "Kilmainham" area union all
select "Dublin 8" postal_dist, "Merchants Quay" area union all
select "Dublin 8" postal_dist, "Portobello" area union all
select "Dublin 8" postal_dist, "South Circular Road" area union all
select "Dublin 8" postal_dist, "The Coombe" area union all
select "Dublin 9" postal_dist, "Beaumont" area union all
select "Dublin 9" postal_dist, "Drumcondra" area union all
select "Dublin 9" postal_dist, "Elm Mount" area union all
select "Dublin 9" postal_dist, "Griffith Avenue" area union all
select "Dublin 9" postal_dist, "Santry" area union all
select "Dublin 9" postal_dist, "Whitehall" area
);

drop view if exists property_price_data_dublin_fix;
create view if not exists property_price_data_dublin_fix as
select postal_dist as town, pd.address
from default.property_price_data pd
join dublin_postal_dist dpd on dpd.area = pd.town or instr(pd.address,dpd.area) > 0
where county='Dublin'
and town not like 'Dublin%';

update property_price_data pd set town = (select max(x.town) from property_price_data_dublin_fix x where x.address=pd.address)
where county='Dublin'
and address in (select y.address from property_price_data_dublin_fix y);

-- COMMAND ----------

drop view if exists dublin_postal_dist;
drop view if exists property_price_data_dublin_fix;

-- COMMAND ----------

-- DBTITLE 1,Count of incorrect towns
select sum(case when cnt = 1 then 1 else 0 end) count1_town,
       sum(case when cnt = 2 then 1 else 0 end ) count2_town
  from (select county, town, count(1) cnt
          from default.property_price_data
         group by county, town
        having count(1) < 3)

-- COMMAND ----------

-- DBTITLE 1,Incorrect town and proposed correct town
with valid_town as(
  select county, town from default.property_price_data
  group by county, town having count(1) >= 5
),
invalid_town as(
  select county, town from default.property_price_data
  group by county, town having count(1) < 3
)
select pd1.county,pd1.address, pd1.town, max(vt.town) as proposed_town
  from default.property_price_data as pd1
  join valid_town vt on vt.county = pd1.county and instr(pd1.address, vt.town || ',') > 0
 where (pd1.county, pd1.town) in (select it1.county, it1.town from invalid_town it1)
 group by pd1.county,pd1.address,pd1.town

-- COMMAND ----------

-- DBTITLE 1,Update of town with correct value - 1
update default.property_price_data
  set  town=trim(regexp_extract(regexp_replace(address,'(Cork.+?$)',''),'^(.+?),(.+?),(.+?)$',2))
where county='Cork' and town in('West Cork','East Cork')
 and length(regexp_replace(address,'(Cork.+?$)',''))-length(replace(regexp_replace(address,'(Cork.+?$)',''),',','')) = 2;
 
update default.property_price_data
  set  town=trim(regexp_extract(regexp_replace(address,'(Cork.+?$)',''),'^(.+?),(.+?),(.+?),(.+?)$',3))
where county='Cork' and town in('West Cork','East Cork')
 and length(regexp_replace(address,'(Cork.+?$)',''))-length(replace(regexp_replace(address,'(Cork.+?$)',''),',','')) = 3;

-- COMMAND ----------

-- DBTITLE 1,Update of town with correct value - 2
create view IF NOT EXISTS valid_town as 
  select county, town from default.property_price_data
  group by county, town having count(1) >= 4;

create view IF NOT EXISTS invalid_town as 
  select county, town from default.property_price_data
  group by county, town having count(1) < 3;
  
create table IF NOT EXISTS property_price_data_town_fix as
select pd1.county, pd1.address, pd1.town, vt.town as proposed_town
  from default.property_price_data as pd1
  join valid_town vt on vt.county = pd1.county and instr(pd1.address, vt.town || ',') > 0
 where (pd1.county, pd1.town) in (select it1.county, it1.town from invalid_town it1);
    
update property_price_data pd 
   set pd.town = (select max(x.proposed_town) 
                    from property_price_data_town_fix x
                   where pd.county = x.county and x.address=pd.address)
where exists (select 'x' 
                from property_price_data_town_fix y 
               where pd.county = y.county and y.address=pd.address);

-- COMMAND ----------

DROP table IF EXISTS property_price_data_town_fix;
DROP VIEW IF EXISTS invalid_town;
DROP VIEW IF EXISTS valid_town;

-- COMMAND ----------

-- DBTITLE 1,Update of town with correct value - 3
create view IF NOT EXISTS valid_town as select county, town from default.property_price_data
  where county!=town
  group by county, town having count(1) >= 4;

create view IF NOT EXISTS invalid_town as select county, town from default.property_price_data
  where county=town
  group by county, town;

create table IF NOT EXISTS property_price_data_town_fix as
select pd1.county, pd1.address, pd1.town, vt.town as proposed_town
  from default.property_price_data as pd1
  join valid_town vt on vt.county = pd1.county and instr(pd1.address, vt.town || ',') > 0
 where (pd1.county, pd1.town) in (select it1.county, it1.town from invalid_town it1);
    
update property_price_data pd 
   set pd.town = (select max(x.proposed_town) 
                    from property_price_data_town_fix x
                   where pd.county = x.county and x.address=pd.address)
where exists (select 'x' 
                from property_price_data_town_fix y 
               where pd.county = y.county and y.address=pd.address);

-- COMMAND ----------

DROP table IF EXISTS property_price_data_town_fix;
DROP VIEW IF EXISTS invalid_town;
DROP VIEW IF EXISTS valid_town;

-- COMMAND ----------

-- DBTITLE 1,Update of town with correct value - 4
create view IF NOT EXISTS valid_town as 
  select county, town from default.property_price_data
  where county='Cork' and town not in('West Cork','East Cork')
  group by county, town having count(1) >= 4;

create view IF NOT EXISTS invalid_town as 
  select "Cork" county, "West Cork" town union all
  select "Cork" county, "East Cork" town;
  
create table IF NOT EXISTS property_price_data_town_fix as
select pd1.county, pd1.address, pd1.town, vt.town as proposed_town
  from default.property_price_data as pd1
  join valid_town vt on vt.county = pd1.county and instr(pd1.address, vt.town || ',') > 0
 where (pd1.county, pd1.town) in (select it1.county, it1.town from invalid_town it1);
    
update property_price_data pd 
   set pd.town = (select max(x.proposed_town) 
                    from property_price_data_town_fix x
                   where pd.county = x.county and x.address=pd.address)
where exists (select 'x' 
                from property_price_data_town_fix y 
               where pd.county = y.county and y.address=pd.address);

-- COMMAND ----------

DROP table IF EXISTS property_price_data_town_fix;
DROP VIEW IF EXISTS invalid_town;
DROP VIEW IF EXISTS valid_town;

-- COMMAND ----------

-- DBTITLE 1,Update of town with correct value - 5
update property_price_data set town='Coolbrock' where county='Wexford' and town='Wellington Bridge';
update property_price_data set town='Coolbrock' where county='Wexford' and town='Wellingtonbridge';
update property_price_data set town='Kilmore' where county='Wexford' and town='Kilmore Village';
update property_price_data set town='Fethard' where county='Wexford' and town='Fethard-On-Sea';

update property_price_data set town='Annascaul' where county='Kerry' and town='Anascaul';

update property_price_data set town='Castletownsend' where county='Cork' and town='Castletownshend';

update property_price_data set town='Curragh' where county='Kildare' and town='The Curragh';

update property_price_data set county='Westmeath' where county='Roscommon' and town='Athlone';
update property_price_data set town='Athlone' where county='Westmeath' and town='Athlone East';

update property_price_data set town='Togher' where county='Cork' and town='Togher (Cork City)';
update property_price_data set town='Cork City' where county='Cork' and town='Cork';

update property_price_data set town='Wexford Town' where county='Wexford' and town='Wexford';
update property_price_data set town='Kildare Town' where county='Kildare' and town='Kildare';
update property_price_data set town='Sligo City' where county='Sligo' and town='Sligo';


-- COMMAND ----------

-- DBTITLE 1,Duplicate data deletion
with dup as (
  select county, price, lower(town) as town, beds, propertyType, min(website) as website,
         lower(regexp_replace(regexp_extract(address,'^(.+?),'),'[^A-Za-z0-9]','')) addressPart
    from default.property_price_data
   group by county, price, lower(town), beds, propertyType,
            lower(regexp_replace(regexp_extract(address,'^(.+?),'),'[^A-Za-z0-9]',''))
  having count(1) > 1 and min(website) != max(website)
)
delete from default.property_price_data as pd
 where exists(
    select 'x'from dup 
    where pd.county = dup.county and pd.price = dup.price and lower(pd.town) = dup.town
      and pd.beds = dup.beds and pd.propertyType = dup.propertyType and pd.website != dup.website
      and lower(regexp_replace(regexp_extract(pd.address,'^(.+?),'),'[^A-Za-z0-9]','')) = dup.addressPart
  )

-- COMMAND ----------

-- DBTITLE 1,Count of incorrect towns after fix
select sum(case when cnt = 1 then 1 else 0 end) count1_town,
       sum(case when cnt = 2 then 1 else 0 end ) count2_town
  from (select county, town, count(1) cnt
          from default.property_price_data
         group by county, town
        having count(1) < 3)

-- COMMAND ----------

-- DBTITLE 1,Example of data with incorrect towns
with invalid_town as(
  select county, town from default.property_price_data
  group by county, town having count(1) < 3
)
select * from default.property_price_data pd
where exists(select 'x' from invalid_town it 
              where it.county=pd.county
                and it.town=pd.town)
Order by county,town;

-- COMMAND ----------

-- DBTITLE 1,Delete towns data with less than 3 entries
with invalid_town as(
  select county, town from default.property_price_data
  group by county, town having count(1) < 3
)
delete from default.property_price_data pd
where exists(select 'x' from invalid_town it 
              where it.county=pd.county
                and it.town=pd.town);

-- COMMAND ----------

-- DBTITLE 1,Final data count after cleaning
select nvl(website,'Total') Website,count(1) as Count
from default.property_price_data
group by website with ROLLUP

-- COMMAND ----------

--RESTORE table default.property_price_data to VERSION AS OF 77
