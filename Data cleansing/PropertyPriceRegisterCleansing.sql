-- Databricks notebook source
-- DBTITLE 1,Create table
CREATE TABLE IF NOT EXISTS property_price_register
  USING DELTA
  LOCATION '/user/hive/warehouse/property_price_register';

-- COMMAND ----------

-- DBTITLE 1,Data count after extract
select count(1) as Count
from default.property_price_register

-- COMMAND ----------

update default.property_price_register
  set  town=trim(regexp_extract(regexp_replace(address,'(Cork.+?$)',''),'^(.+?),(.+?),(.+?)$',2))
where county='Cork' and town in('West Cork','East Cork')
 and length(regexp_replace(address,'(Cork.+?$)',''))-length(replace(regexp_replace(address,'(Cork.+?$)',''),',','')) = 2;
 
update default.property_price_register
  set  town=trim(regexp_extract(regexp_replace(address,'(Cork.+?$)',''),'^(.+?),(.+?),(.+?),(.+?)$',3))
where county='Cork' and town in('West Cork','East Cork')
 and length(regexp_replace(address,'(Cork.+?$)',''))-length(replace(regexp_replace(address,'(Cork.+?$)',''),',','')) = 3;

update default.property_price_register
set town = trim(regexp_extract(address,'^(.+?),(.+?),(.+?)$',3))
where (town like 'Dublin Rd%' or town like 'Dublin Road%')
and address like '%,%Dublin R%,%';

update default.property_price_register
set town = trim(regexp_extract(address,'^(.+?),(.+?),(.+?)$',2))
where (town like '%Dublin Rd' or town like '%Dublin Road')
and length(address)-length(replace(address,',','')) = 2;

update default.property_price_register
set town = 'Ballyjamesduff'
where county='Cavan' 
and town = 'Dublin St'
and address like '%Ballyjamesduff';

update default.property_price_register
set town = 'Athlone'
where county='Westmeath' and town='Dublin Rd Athlone';

update default.property_price_register
set town = trim(regexp_extract(address,'^(.+?),(.+?),(.+?)$',2))
where county!='Dublin' and town like 'Dublin R%d%Drogheda'
and address like '%Dublin R%d%Drogheda';

update default.property_price_register
set town = 'Dublin 6W' 
where county='Dublin' and town like '% Dublin 6W';

update property_price_register set town='Dublin 6W' 
where county='Dublin' 
and town in('Dublin 6w','Dublin6W','Dublin 6 W');

update default.property_price_register
set town = 'Glenageary' 
where county='Dublin' and town like 'Upper Glenageary Road';

update default.property_price_register
set town = trim(regexp_extract(address,'^(.+?),(.+?),(.+?)$',2))
where county='Dublin' and town like 'Station Road'
and address like '%Station Road'
and length(address)-length(replace(address,',','')) = 2;


update default.property_price_register
set town = 'Dun Laoghaire' 
where county='Dublin' and town like 'Dun%aoghaire';

update property_price_register set town=replace(town,' Rd',' Road') where instr(town,' Rd') > 0;
update property_price_register set town=replace(town,' St',' Street') where instr(town,' St') > 0;

-- COMMAND ----------

update property_price_register set town='Carlow Town' where county='Carlow'  and town=county;
update property_price_register set town='Cavan Town' where county='Cavan'  and town=county;
update property_price_register set town='Cork City' where county='Cork'  and town=county;
update property_price_register set town='Donegal Town' where county='Donegal'  and town=county;
update property_price_register set town='Galway City' where county='Galway'  and town=county;
update property_price_register set town='Kildare Town' where county='Kildare'  and town=county;
update property_price_register set town='Kilkenny City' where county='Kilkenny'  and town=county;
update property_price_register set town='Leitrim Village' where county='Leitrim'  and town=county;
update property_price_register set town='Limerick City' where county='Limerick'  and town=county;
update property_price_register set town='Longford Town' where county='Longford'  and town=county;
update property_price_register set town='Louth Town' where county='Louth'  and town=county;
update property_price_register set town='Monaghan Town' where county='Monaghan'  and town=county;
update property_price_register set town='Roscommon Town' where county='Roscommon'  and town=county;
update property_price_register set town='Sligo City' where county='Sligo'  and town=county;
update property_price_register set town='Tipperary Town' where county='Tipperary'  and town=county;
update property_price_register set town='Waterford City' where county='Waterford'  and town=county;
update property_price_register set town='Wexford Town' where county='Wexford'  and town=county;
update property_price_register set town='Wicklow Town' where county='Wicklow'  and town=county;
update property_price_register set town='Sligo City' where county='Sligo'  and town='Sligo Town';

-- COMMAND ----------

create table if not exists property_price_dublin_town_fix as
select a.county,address,a.town, max(t.town) new_town
from default.property_price_register a
join towns t on t.county = a.county and instr(a.address,t.town) > 0
where a.county='Dublin'
and t.town like 'Dublin%'
and t.town != t.county
and (a.county,a.town) not in (select t1.county, t1.town from towns t1)
group by a.county,address,a.town
union 
select a.county,address,a.town, max(t.town) new_town
from default.property_price_register a
join towns t on t.county = a.county and instr(a.address,t.town) > 0
where a.county='Dublin'
and t.town not like 'Dublin%'
and (a.county,a.town) not in (select t1.county, t1.town from towns t1)
group by a.county,address,a.town;

update property_price_register pd set town = (select max(x.new_town) from property_price_dublin_town_fix x where x.address=pd.address)
where county='Dublin'
and address in (select y.address from property_price_dublin_town_fix y);

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
select "Dublin 15" postal_dist, "Hansfield" area union all
select "Dublin 16" postal_dist, "Ballinteer" area union all
select "Dublin 16" postal_dist, "Ballyboden" area union all
select "Dublin 16" postal_dist, "Knocklyon" area union all
select "Dublin 16" postal_dist, "Sandyford" area union all
select "Dublin 16" postal_dist, "Rathfarnham" area union all
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
from default.property_price_register pd
join dublin_postal_dist dpd on dpd.area = pd.town or instr(pd.address,dpd.area) > 0
where county='Dublin'
and town not like 'Dublin%';

update property_price_register pd set town = (select max(x.town) from property_price_data_dublin_fix x where x.address=pd.address)
where county='Dublin'
and address in (select y.address from property_price_data_dublin_fix y);


-- COMMAND ----------

drop view if exists dublin_postal_dist;
drop view if exists property_price_data_dublin_fix;
drop table if exists property_price_dublin_town_fix;

-- COMMAND ----------

-- DBTITLE 1,Count of incorrect towns
select sum(case when cnt = 1 then 1 else 0 end) count1_town,
       sum(case when cnt = 2 then 1 else 0 end ) count2_town
  from (select county, town, count(1) cnt
          from default.property_price_register
         group by county, town
        having count(1) < 3)

-- COMMAND ----------

-- DBTITLE 1,Incorrect town and proposed correct town
with valid_town as(
  select county, town from default.property_price_register
  group by county, town having count(1) >= 5
),
invalid_town as(
  select county, town from default.property_price_register
  group by county, town having count(1) < 3
)
select pd1.county,pd1.address, pd1.town, max(vt.town) as proposed_town
  from default.property_price_register as pd1
  join valid_town vt on vt.county = pd1.county and instr(pd1.address, vt.town || ',') > 0
 where (pd1.county, pd1.town) in (select it1.county, it1.town from invalid_town it1)
 group by pd1.county,pd1.address,pd1.town

-- COMMAND ----------

-- DBTITLE 1,Update of town with correct value - 2
create table if not exists property_price_town_fix1 as 
select a.county,address,a.town, max(t.town) new_tow
from default.property_price_register a
join towns t on t.county = a.county and instr(a.address,t.town) > 0
where a.county!='Dublin'
and t.county!=t.town
and (a.county,a.town) not in (select t1.county, t1.town from towns t1)
group by a.county,address,a.town;


update property_price_register pd set town = (select max(new_tow) from property_price_town_fix1 x where x.address=pd.address)
where county!='Dublin'
and address in (select y.address from property_price_town_fix1 y);

create view IF NOT EXISTS valid_town as 
  select county, town from default.property_price_register
  group by county, town having count(1) >= 4;

create view IF NOT EXISTS invalid_town as 
  select county, town from default.property_price_register
  group by county, town having count(1) < 3;
  
create table IF NOT EXISTS property_price_town_fix2 as
select pd1.county, pd1.address, pd1.town, vt.town as proposed_town
  from default.property_price_register as pd1
  join valid_town vt on vt.county = pd1.county and instr(pd1.address, vt.town || ',') > 0
 where (pd1.county, pd1.town) in (select it1.county, it1.town from invalid_town it1);
    
update property_price_register pd 
   set pd.town = (select max(x.proposed_town) 
                    from property_price_town_fix2 x
                   where pd.county = x.county and x.address=pd.address)
where exists (select 'x' 
                from property_price_town_fix2 y 
               where pd.county = y.county and y.address=pd.address);

-- COMMAND ----------

DROP table IF EXISTS property_price_town_fix2;
DROP table IF EXISTS property_price_town_fix1;
DROP VIEW IF EXISTS invalid_town;
DROP VIEW IF EXISTS valid_town;

-- COMMAND ----------

-- DBTITLE 1,Update of town with correct value - 3
create view IF NOT EXISTS valid_town as select county, town from default.property_price_register
  where county!=town
  and town not in('Street','Road','Td')
  group by county, town having count(1) >= 4;

create view IF NOT EXISTS invalid_town as select county, town from default.property_price_register
  where county=town
  and town in('Street','Road','Td')
  group by county, town;

create table IF NOT EXISTS property_price_data_town_fix as
select pd1.county, pd1.address, pd1.town, vt.town as proposed_town
  from default.property_price_register as pd1
  join valid_town vt on vt.county = pd1.county and instr(pd1.address, vt.town || ',') > 0
 where (pd1.county, pd1.town) in (select it1.county, it1.town from invalid_town it1);
    
update property_price_register pd 
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

DROP table IF EXISTS property_price_data_town_fix;
DROP VIEW IF EXISTS invalid_town;
DROP VIEW IF EXISTS valid_town;

-- COMMAND ----------

-- DBTITLE 1,Update of town with correct value - 5
update property_price_register set town='Coolbrock' where county='Wexford' and town='Wellington Bridge';
update property_price_register set town='Coolbrock' where county='Wexford' and town='Wellingtonbridge';
update property_price_register set town='Kilmore' where county='Wexford' and town='Kilmore Village';
update property_price_register set town='Fethard' where county='Wexford' and town='Fethard-On-Sea';

update property_price_register set town='Annascaul' where county='Kerry' and town='Anascaul';

update property_price_register set town='Castletownsend' where county='Cork' and town='Castletownshend';

update property_price_register set town='Curragh' where county='Kildare' and town='The Curragh';

update property_price_register set county='Westmeath' where county='Roscommon' and town='Athlone';
update property_price_register set town='Athlone' where county='Westmeath' and town='Athlone East';

update property_price_register set town='Togher' where county='Cork' and town='Togher (Cork City)';
update property_price_register set town='Cork City' where county='Cork' and town='Cork';


-- COMMAND ----------

-- DBTITLE 1,Count of incorrect towns after fix
select sum(case when cnt = 1 then 1 else 0 end) count1_town,
       sum(case when cnt = 2 then 1 else 0 end ) count2_town
  from (select county, town, count(1) cnt
          from default.property_price_register
         group by county, town
        having count(1) < 3)

-- COMMAND ----------

drop table if exists towns_with_same_coordinates;
create table if not exists towns_with_same_coordinates as
select a.county, a.min_tn_count,b.max_tn_count,
case when a.min_tn_count > b.max_tn_count then a.min_town else b.max_town end as new_town,
case when a.min_tn_count > b.max_tn_count then b.max_town else a.min_town end as now_town
from 
(select x1.latitude, x1.longitude, x1.county,x1.min_town,count(1) min_tn_count
from property_price_register p
join (select latitude, longitude,county, count(1), min(town) min_town,max(town) max_town
from towns t
group by latitude, longitude,county
having count(1) = 2) x1 on p.county=x1.county and p.town=x1.min_town
group by x1.latitude, x1.longitude, x1.county,x1.min_town) a,
(select x2.latitude, x2.longitude, x2.county,x2.max_town,count(1) max_tn_count
from property_price_register p
join (select latitude, longitude,county, count(1), min(town) min_town,max(town) max_town
from towns t
group by latitude, longitude,county
having count(1) = 2) x2 on p.county=x2.county and p.town=x2.max_town
group by x2.latitude, x2.longitude, x2.county,x2.max_town) b
where a.latitude = b.latitude
and a.longitude = b.longitude
and a.county = b.county;

update property_price_register p set town = (
select max(t.new_town) from towns_with_same_coordinates t where t.county=p.county and t.now_town=p.town)
where exists(select 'x' from towns_with_same_coordinates t where t.county=p.county and t.now_town=p.town);

-- COMMAND ----------

drop table if exists towns_with_same_coordinates;

-- COMMAND ----------

-- DBTITLE 1,Delete towns data with less than 3 entries
with invalid_town as(
  select county, town, saleMonth from default.property_price_register
  group by county, town, saleMonth having count(1) < 3
)
delete from default.property_price_register pd
where exists(select 'x' from invalid_town it 
              where it.county=pd.county
                and it.town=pd.town
                and it.saleMonth=pd.saleMonth);

-- COMMAND ----------

-- DBTITLE 1,Final data count after cleaning
select county,town,count(1) as cnt
from default.property_price_register
where (county,town) not in (select county, town from towns)
group by county,town
order by cnt desc


-- COMMAND ----------

--RESTORE table default.property_price_data to VERSION AS OF 77