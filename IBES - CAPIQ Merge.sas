libname CapIQ "G:\My Drive\Research\FLP_Analyst_Soft_Skills\FLP_Analyst_Skill_Shared\Data\Capital IQ";
libname CapIQadj "G:\My Drive\Research\FLP_Analyst_Soft_Skills\FLP_Analyst_Skill_Shared\Data\Capital IQ\Adjusted";
libname ibes "G:\My Drive\Research\RA Stuff\SAS\Data Files\IBES";
libname dat "G:\My Drive\Research\RA Stuff\SAS\Data Files";
libname db "C:\Users\flakej\Dropbox\LZ_information\analysis\capitaliq_transcripts";
libname db_match "C:\Users\flakej\Dropbox\LZ_information\analysis\capitaliq_ibes_match";


* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
*Step 1: Get analyst-firm-years from Capital IQ;
* - Start with analyst-event level dataset from "Sample CapIQ Code_MP.sas" (capiq_analystq_transcript.sas7bdat);
* - Merge in transcriptpersonname and companyname (companyofperson);
* - Export transcriptpersonid,gvkey, year observations to Python to format last name to merge with IBES;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
/* 
See Part 1 of "Sample CapIQ Code_MP.sas"
*/

data work.capiq_analystquestions; set CapIQadj.capiq_analystq_transcript;
run;

proc sort data = capiq_analystquestions out = analyst_transcript nodupkey; 
by transcriptid transcriptpersonid;
run;

data company_analyst; set analyst_transcript (keep = transcriptpersonid proid companyid gvkey mostimportantdateutc);
year = year(mostimportantdateutc);
drop mostimportantdateutc;
run;

* Merge in analyst name and analyst's company name;
proc sql;
	create table ciqafy as select distinct
	a.*, b.transcriptpersonname, coalescec(b.companyname,c.companyname) as companyofperson
	from company_analyst as a 
		left join capiq.ciqtranscriptperson as b on a.transcriptpersonid = b.transcriptpersonid
		left join db.wrds_professional as c on a.proid = c.proid
order by gvkey, transcriptpersonid, companyofperson, year;
quit;

* Go from analyst-event level data to analyst-gvkey-year;
proc sort data = ciqafy out = ciqafy2 nodupkey; 
by gvkey transcriptpersonid companyofperson year;
run;

data capiqAdj.CIQAnalystFirmYear; set ciqAFY2;
where transcriptpersonname not in ("Unknown Analyst","Unkown Analyst","Unidentified Audience Member");
run;
* 794k analyst, firm, years;

* Unique transcriptpersonid's;
proc sort data = ciqAFY2 out = CIQAnalysts (
keep = transcriptpersonid transcriptpersonname companyofperson) nodupkey; 
by transcriptpersonid companyofperson;
run;

data capiqAdj.CIQAnalysts; set CIQAnalysts;
where transcriptpersonname not in ("Unknown Analyst","Unidentified Audience Member");
run;
* 112,764 unique transcriptpersonid's;

* Unique companyofperson's;
proc sort data = ciqAFY2 out = CIQBrokers (
keep = companyofperson) nodupkey; 
by companyofperson;
run;

data capiqAdj.CIQBrokers; set CIQBrokers;
where companyofperson ne "";
run;
* 16,399 unique companyofperson;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
*Step 2: Get analyst-firm-years from IBES;
* - Supplement PT data with Rec data to get a more complete panel for each analyst-firm pair 
	(This step may be excessive);
* - Bring in permno from ICLINK_FEB_2021.sas7bdat;
* - Upload unique permno-years to WRDS to merge in GVKEY using CRSP-Compustat linking table;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
data ptafy; set ibes.ptgdet (keep = ticker estimid amaskcd alysnam anndats USFIRM);
where USFIRM = 1;
YEAR = year(anndats);
drop USFIRM;
run;

proc sort data = ptafy out = ptafy2 nodupkey; by ticker estimid amaskcd alysnam year;
run;

data recafy; set ibes.recddet (keep = ticker estimid amaskcd analyst anndats USFIRM);
where USFIRM = 1;
year = year(anndats);
drop USFIRM;
run;

proc sort data = recafy out = recafy2 nodupkey; by ticker estimid amaskcd analyst year;
run;

data epsafy; set ibes.det_epsus_20180426 (keep = ticker analys anndats);
year = year(anndats);
run;

proc sort data = epsafy out = epsafy2 nodupkey; by ticker analys year;
run;

proc sql;
	create table epsafy3 as select
	a.*, b.alysnam as eps_analyst, b.estimid
	from epsafy2 as a left join ptafy2 as b
	on a.ticker = b.ticker
	and a.analys = b.amaskcd;
quit;

proc sort data = epsafy3 out = epsafy4 nodupkey; by ticker estimid analys eps_analyst year;
run;

proc sql;
	create table ibesafy as select
	a.ticker as pt_ticker, a.estimid as pt_estimid, a.alysnam as pt_analyst, 
		a.amaskcd as pt_amaskcd, a.year as pt_year,
	b.ticker as rec_ticker, b.estimid as rec_estimid, b.analyst as rec_analyst, 
		b.amaskcd as rec_amaskcd, b.year as rec_year
/*	,c.ticker as eps_ticker, c.estimid as eps_estimid, c.eps_analyst, */
/*		c.analys as eps_amaskcd, c.year as eps_year*/
	from ptafy2 as a 
	full join recafy2 as b on a.ticker = b.ticker and a.amaskcd = b.amaskcd	and a.estimid = b.estimid and a.year = b.year
/*	full join epsafy4 as c on a.ticker = c.ticker and a.amaskcd = c.analys	and a.estimid = c.estimid and a.year = c.year*/;
quit;

data ibesafy2; set ibesafy;
ticker = pt_ticker;
if missing(ticker) then ticker = rec_ticker;
/*if missing(ticker) then ticker = eps_ticker;*/
estimid = pt_estimid;
if estimid = "" then estimid = rec_estimid;
/*if estimid = "" then estimid = eps_estimid;*/
analyst = pt_analyst;
if analyst = "" then analyst = rec_analyst;
/*if analyst = "" then analyst = eps_analyst;*/
amaskcd = pt_amaskcd;
if missing(amaskcd) then amaskcd = rec_amaskcd;
/*if missing(amaskcd) then amaskcd = eps_amaskcd;*/
year = pt_year;
if year = . then year = rec_year;
/*if year = . then year = eps_year;*/
drop pt_ticker pt_estimid pt_analyst pt_amaskcd pt_year 
	rec_ticker rec_estimid rec_analyst rec_amaskcd rec_year
/*	eps_ticker eps_estimid eps_analyst eps_amaskcd eps_year*/ ;
if analyst ne "";
run;

proc sort data = ibesafy2 out = ibesafy3 nodupkey; by ticker estimid analyst amaskcd year;
run;

proc sql;
	create table ibesafy4 as select
	a.*, b.permno
	from ibesafy3 as a left join dat.iclink_feb_2021 as b
	on a.ticker = b.ticker;
quit;

data ibesafy5; set ibesafy4;
where permno ne .;
run;

proc sort data = ibesafy5 out = ibesafy6 nodupkey; by ticker permno estimid analyst amaskcd year;
run;

proc sort data = ibesafy6 out = permnos (keep = permno year) nodupkey; by permno year;
run;

proc printto log = junk; run;
%let wrds = wrds.wharton.upenn.edu 4016; 
options comamid=TCP remote=WRDS;
signon username= _prompt_;
proc printto; run;
Libname rwork slibref=work server=wrds;

rsubmit;
proc upload data = permnos;
run;
endrsubmit;

rsubmit;
*Get gvkey;
proc sql; create table firm_ids as select distinct
	a.*, b.gvkey 
    from permnos as a left join crsp.ccmxpf_linktable
	(where=(linktype in ('LU', 'LC', 'LD', 'LF', 'LN', 'LO', 'LS', 'LX'))) as b
    on (a.permno = b.lpermno) 
	and (year(b.linkdt) <= a.year or b.linkdt = .B) 
	and (a.year <= year(b.linkenddt) or b.linkenddt = .E);
 	quit;

proc sort data = firm_ids nodupkey; by permno gvkey year;
where permno ne . and gvkey ne "";
run;
endrsubmit;

rsubmit;
proc download data = firm_ids;
run;
endrsubmit;
signoff;

proc sql;
	create table ibesafy7 as select
	a.*, b.gvkey
	from ibesafy6 as a left join firm_ids as b
	on a.permno = b.permno
	and a.year = b.year;
quit;

proc sort data = ibesafy7 nodupkey; by ticker permno gvkey estimid analyst amaskcd year;
run;

data ibesafy8; set ibesafy7;
if ticker ne "" and permno ne . and gvkey ne "" and estimid ne "" and analyst ne "" and amaskcd ne . and year ne .;
run;

* Save to permanent folder;
* Unique analyst-firm-year observations;
data capiqadj.IBESanalystFirmYear; set ibesafy8;
where analyst ne "RESEARCH DEPARTMEN";
run;

* Unique analyst observations;
proc sort data = ibesafy8 out = IBESAnalysts 
(keep = amaskcd analyst) nodupkey; 
by amaskcd;
run;

data capiqAdj.IBESAnalysts; set IBESAnalysts;
where analyst ne "RESEARCH DEPARTMEN";
run;
* 18,539 unique amaskcd's;

* Unique broker observations;
proc sort data = ibesafy8 out = IBESBrokers 
(keep = estimid) nodupkey; 
by estimid;
run;
* 1,160 unique estimid's;

data capiqAdj.IBESBrokers; set IBESBrokers;
run;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
*Step 3: Take to Python to format names for merge;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
*** "G:/My Drive/Jupyter Notebooks/Research/CIQ_IBES_PrepMerge.ipynb
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
*Step 4: Merge on analyst, firm, years;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
%let ciqfile = "G:\My Drive\Research\FLP_Analyst_Soft_Skills\FLP_Analyst_Skill_Shared\Data\Capital IQ\Adjusted\ciqAFY_FmtdNms.csv";

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

%let ibesfile = "G:\My Drive\Research\FLP_Analyst_Soft_Skills\FLP_Analyst_Skill_Shared\Data\Capital IQ\Adjusted\ibesAFY_FmtdNms.csv";

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

proc sort data = brokers2 out = brokers3 nodupkey; by companyofperson;
run;
* 4,066 unique company of person matched to 1 estimid;

proc sort data=brokers3 out=brokers4; by estimid descending n_matches;
run;

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
	from capiqadj.ciqbrokers as a left join brokers6 as b
	on a.companyofperson = b.companyofperson
	order by estimid,pct_total desc;
quit;

data capiqadj.CIQ_IBESBrokerTranslation; set brokers7;
run;
data db_match.CIQ_IBESBrokerTranslation_V2; set brokers7;
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

data capiqadj.CIQ_IBESAnalystTranslation; set analysts;
run;
data db_match.CIQ_IBESAnalystTranslation_V2; set analysts;
run;


proc sql;
	select count(*) as Total_CompanyOfPerson, count(estimid) as Total_Matched
	from capiqadj.ciq_ibesbrokertranslation;
quit;



proc sort data = capiqadj.ibesanalystfirmyear out = ibes_broker_years (keep = estimid year) nodupkey; by estimid year;
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

proc sort data = capiqadj.ciqanalystfirmyear out = ciq_broker_years (keep = companyofperson year) nodupkey; by companyofperson year;
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

proc sort data = NBrokersYears out = capiqadj.NBrokerYears; by year;
run;
