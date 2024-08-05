* Read in user inputs;
/*Need to change the path here*/
%include "C:\Users\flakej\Dropbox\GitHub\CapIQ_IBES_Match\inputs.sas";
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
/* EPS data made things messy so stuck with recommendation and target price observations for now
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
*/
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
	from ibesafy3 as a left join ibes.&iclink as b
	on a.ticker = b.ticker
	and (year(b.sdate) le a.year or sdate = .)
	and (a.year le year(b.edate) or edate = .);
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
* restrict to firms with valid identifiers for CRSP, COMPUSTAT, and IBES;
data ibesafy8; set ibesafy7;
if ticker ne "" and permno ne . and gvkey ne "" and estimid ne "" and analyst ne "" and amaskcd ne . and year ne .;
run;

* Save to permanent folder;
* Unique analyst-firm-year observations;
data adj.IBESanalystFirmYear; set ibesafy8;
where analyst ne "RESEARCH DEPARTMEN";
run;

* Unique analyst observations;
proc sort data = ibesafy8 out = IBESAnalysts 
(keep = amaskcd analyst) nodupkey; 
by amaskcd;
run;

data adj.IBESAnalysts; set IBESAnalysts;
where analyst ne "RESEARCH DEPARTMEN";
run;
* 18,539 unique amaskcd's;

* Unique broker observations;
proc sort data = ibesafy8 out = IBESBrokers 
(keep = estimid) nodupkey; 
by estimid;
run;
* 1,160 unique estimid's;

data adj.IBESBrokers; set IBESBrokers;
run;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
*Step 3: Take to Python to format names for merge;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* ibes_capiq_format_names.ipynb
