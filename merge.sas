* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
*Step 4: Merge on analyst, firm, years;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
%include "C:\Users\flakej\Dropbox\GitHub\CapIQ_IBES_Match\inputs.sas";

data ciqafy_merge;
	%let _EFIERR_ = 0;	/* set the ERROR detection macro variable */
	infile &ciqfile delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
	informat transcriptpersonid Best12.;
	informat proid Best12.;
	informat companyofperson $94.;
	informat year Best12.;
	informat gvkey $6.;
	informat fname $19.;
	informat lname $22.;
	format transcriptpersonid Best12.;
	format proid Best12.;
	format companyofperson $94.;
	format year Best12.;
	format gvkey $6.;
	format fname $19.;
	format lname $22.;
	input
		transcriptpersonid
		proid
		companyofperson
		year
		gvkey
		fname
		lname
;
if _ERROR_ then call symputx('_EFIERR_',1);
run;

data ibesafy_merge;
	%let _EFIERR_ = 0;	/* set the ERROR detection macro variable */
	infile &ibesfile delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
	informat ticker $6.;
	informat estimid $8.;
	informat amaskcd $8.;
	informat year Best12.;
	informat permno Best12.;
	informat gvkey $6.;
	informat analyst $20.;
	informat lname1 $17.;
	informat lname2 $12.;
	format ticker $6.;
	format estimid $8.;
	format amaskcd $8.;
	format year Best12.;
	format permno Best12.;
	format gvkey $6.;
	format analyst $20.;
	format lname1 $18.;
	format lname1 $12.;
	input
		ticker
		estimid
		amaskcd
		year
		permno
		gvkey
		analyst
		lname1
		lname2
;
if _ERROR_ then call symputx('_EFIERR_',1);
run;

* Prepare merge;
proc sort data = ciqafy_merge nodupkey; by transcriptpersonid companyofperson gvkey year lname;
where lname ne "" and gvkey ne "" and year ne .;
run;

proc sort data = ibesafy_merge nodupkey; by amaskcd estimid gvkey year lname1;
where lname1 ne "" and gvkey ne "" and year ne .;
run;

proc sql;
	create table merged as select
	a.*, b.ticker, b.estimid, b.amaskcd, b.permno, b.gvkey as ibes_gvkey,
	b.year as ibes_year, b.lname1 as ibes_lname1, b.lname2 as ibes_lname2, b.analyst
	from ciqafy_merge as a full join ibesafy_merge as b
	on a.gvkey = b.gvkey
	and (a.lname = b.lname1 or a.lname = b.lname2)
	and a.year = b.year;
quit;

data merged2; set merged;
if year ne . and ibes_year ne . then _merge = 1;
else _merge = 0;
if year = . then year = ibes_year;
if gvkey = "" then gvkey = ibes_gvkey;
drop ibes_year ibes_gvkey;
run;

proc sort data = merged2 out = merged3 nodupkey; by transcriptpersonid amaskcd companyofperson estimid gvkey year;
where _merge = 1;
run;

* Count the number of analyst-firm-year matches at the companyofperson level to prepare to map estimid to companyofperson
    - where last name, company and year are exact matches
    - If an analyst switches brokers midyear, they may create two matches between the esimid and the companyofperson
    - We keep the most frequent match betweem companyofperson and estimid as the valid match;
proc sql;
	create table brokers2 as select distinct
	companyofperson, estimid,
	count(companyofperson) as n_matches
	from merged3
	group by companyofperson, estimid;
quit;

proc sort data=brokers2;
	by companyofperson descending n_matches;
	where companyofperson ne "";
run;
* keep estimid - companyofperson match with the most number of matches;
proc sort data = brokers2 out = brokers3 nodupkey; by companyofperson;
run;
* 4,066 unique company of person matched to 1 estimid;

proc sort data=brokers3 out=brokers4; by estimid descending n_matches;
run;
* companyofperson contains multiple variations of the same name. To fix this, we get the importance of each companyofperson
    for the estimid;
proc sql;
	create table brokers5 as select distinct *,
	sum(n_matches) as total_estimid
	from brokers4
	group by estimid;
quit;

proc sort data=brokers5; by estimid descending n_matches;
run;

data brokers6; set brokers5;
	pct_total = n_matches / total_estimid;
run;

proc sql;
	create table brokers7 as select
	a.*, b.*
	from adj.ciqbrokers as a left join brokers6 as b
	on a.companyofperson = b.companyofperson
	order by estimid,pct_total desc;
quit;

data adj.CIQ_IBESBrokerMerge_&date; set brokers7;
run;


proc sql;
	create table analysts as select distinct
	proid, transcriptpersonid, amaskcd,
	count(transcriptpersonid) as n_matches
	from merged3
	group by transcriptpersonid, amaskcd;
quit;

proc sort data=analysts;
	by transcriptpersonid descending n_matches;
	where transcriptpersonid ne .;
run;

proc sort data=analysts nodupkey;
	by transcriptpersonid;
	where transcriptpersonid ne .;
run;

data ad.CIQ_IBESAnalystMerge_&date; set analysts;
run;
data db_match.CIQ_IBESAnalystMerge_&date; set analysts;
run;


proc sql;
	select count(*) as Total_CompanyOfPerson, count(estimid) as Total_Matched
	from adj.ciq_ibesbrokerMerge_&date;
quit;



proc sort data = adj.ibesanalystfirmyear out = ibes_broker_years (keep = estimid year) nodupkey; by estimid year;
run;

proc sql;
	create table ibes_NBrokers_years as select
	year, count(estimid) as ibes_NBrokers
	from ibes_broker_years group by year;
quit;

proc means data = ibes_broker_years n;
var year;
class year;
run;

proc sort data = adj.ciqanalystfirmyear out = ciq_broker_years (keep = companyofperson year) nodupkey; by companyofperson year;
run;

proc sql;
	create table ciq_NBrokers_years as select
	year, count(companyofperson) as CIQ_NBrokers
	from ciq_broker_years group by year;
quit;

proc means data = ciq_broker_years n;
var year;
class year;
run;

proc sql;
	create table NBrokersYears as select
	a.year, a.ibes_NBrokers, b.year as CIQYear, b.ciq_NBrokers
	from ibes_Nbrokers_years as a full join ciq_Nbrokers_years as b
	on a.year = b.year
	order by a.year;
quit;

data NBrokersYears; set NBrokersYears;
if year = . then year = CIQYear;
drop CIQYear;
run;

proc sort data = NBrokersYears out = adj.NBrokerYears; by year;
run;
