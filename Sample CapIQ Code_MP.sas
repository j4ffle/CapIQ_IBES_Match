
*Last updated: January 9, 2021;

* Mark's Path;
libname CapIQ "G:\My Drive\BC Accounting PhD Files\Research Projects\FLP_Analyst_Skill_Shared\Data\Capital IQ";

/* Jared's Path
libname CapIQ "G:\My Drive\Research\FLP_Analyst_Soft_Skills\FLP_Analyst_Skill_Shared\Data\Capital IQ";
libname CapIQadj "G:\My Drive\Research\FLP_Analyst_Soft_Skills\FLP_Analyst_Skill_Shared\Data\Capital IQ\Adjusted";
libname bfp "G:\My Drive\Research\FP - Analyst Reports and Future Rec Changes\Data\SAS Datasets";
*/



*Count # obs in transcript_person dataset;
proc sql;
 select count(*) as N from CapIQ.wrds_transcript_person;
quit;

/*
***Keep analyst question obs and drop character variables from transcript_person dataset;
data work.capiq_analystquestions; set CapIQ.wrds_transcript_person
	(drop = transcriptcomponenttypename transcriptpersonname companyofperson speakertypename componenttextpreview);	
	where transcriptcomponenttypeid=3 and speakertypeid=3;
run;
*Note1: Need to use "Unicode Support" version of SAS for encoding reasons with characters;
*Note2: transcriptcomponenttypeid=3 is Question; 
*Note3: speakertypeid=3 is Analyst;
*Note4: Dropped all long character variables;

**Save analyst questions dataset to data folder; 
data CapIQ.capiq_analystquestions; set work.capiq_analystquestions;
run;
*/
data work.capiq_analystquestions; set CapIQ.capiq_analystquestions;
run;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Part 1: Summarize analysts' participation in each transcript;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

***Create variables for # questions per analyst per transcript, and total word count per analyst per transcript;
proc sql;
	create table capiq_analystq_transcript
	as select *, 
	n(transcriptcomponentid) as num_analystq_transcript,
	sum(word_count) as wordcount_analystq_transcript
	from work.capiq_analystquestions
	group by transcriptid, transcriptpersonid;
quit;
***ADD SCREEN FOR SHORT QUESTIONS? Look at other calls;
**Maybe keep text preview to see what these Q's are;


**Keep one obs per analyst per transcript;
proc sort data=capiq_analystq_transcript out=capiq_analystq_transcript_1 nodupkey;
by transcriptid transcriptpersonid;
quit;

*Drop question-specific variables;
data capiq_analystq_transcript_2; set capiq_analystq_transcript_1
(drop = transcriptcomponentid componentorder transcriptcomponenttypeid speakertypeid word_count);
run;
*Note: "proid" is "Unique identifier for a professional". How to use?;



***Keep FINAL transcripts with non-missing company id from transcript_detail dataset;
data work.capiq_events; set CapIQ.Wrds_transcript_detail;
	where companyid ne . and transcriptpresentationtypename = "Final";
run;
*890k obs;

**Keep relevant event types (see below);
data capiq_events_1; set capiq_events;
where keydeveventtypeid=48 or keydeveventtypeid=49 or keydeveventtypeid=50 or
	  keydeveventtypeid=51 or keydeveventtypeid=52 or keydeveventtypeid=192;
run;
*Keeping Earnings Calls (48), Guidance/Update Calls (49), Shareholder/Analyst Calls (50),
         Conference Presentation Calls (51), M&A Calls (52), and Analyst/Investor Day (192);
*850k obs;

**Keep one obs per keydevid (duplicates for different versions of same transcript - e.g., edited, proofed, audited);

*Perform sort keeping newest transcript per event on top;
proc sort data=capiq_events_1; 
by companyid keydevid descending transcriptcreationdate_utc descending transcriptcreationtime_utc;
run;
*CapIQ guide says "When in doubt, the version with the most recent time stamp is the most accurate";
*Note: I think sorting on "descending transcriptid" instead of time stamps would also work;

*Drop duplicate transcripts for same event;
proc sort data=capiq_events_1 out=capiq_events_2 nodupkey;
by companyid keydevid;
run;
*313k obs;

**Keep relevant variables;
data capiq_events_3; set capiq_events_2
(drop = keydeveventtypename companyname transcriptcollectiontypeid transcriptcollectiontypename
		transcriptpresentationtypeid transcriptpresentationtypename
		transcriptcreationdate_utc transcriptcreationtime_utc audiolengthsec 
		isdelayed_flag delayreasontypeid delayreasontypename);
run;
*Check for duplicates?;


***Merge events data with analyst question data;

*Sort by transcriptid;
proc sort data=capiq_analystq_transcript_2; by transcriptid;
run;
proc sort data=capiq_events_3; by transcriptid;
run;

**Perform merge;
proc sql;
	create table capiq_analystq_transcript_3 as select distinct a.*, b.*
	from capiq_analystq_transcript_2 as a inner join capiq_events_3 as b
	on a.transcriptid = b.transcriptid;
quit;

proc sort data=capiq_analystq_transcript_3 out=capiq_analystq_transcript_4 nodupkey;
by transcriptid transcriptpersonid;
quit;
*1.6M obs;


proc sql;
	create table capiq_analystq_transcript_6 as select distinct a.*, b.*
	from capiq_analystq_transcript_5 as a left join CapIQ.WRDS_gvkey as b
	on a.companyid = b.companyid
	and (year(b.startdate) le year(a.mostimportantdateutc) or b.startdate = .B)
	and (year(a.mostimportantdateutc) le year(b.enddate) or b.enddate = .E);
quit;
*Need to look into start and end dates for doing merge;

data capiq_analystq_transcript_7; set capiq_analystq_transcript_6;
where gvkey ne " ";
run;
* 1,601,802 observations with no date restriction on GVKEY merge;
* 1,577,798 observations with date restriction on GVKEY merge;

proc sort data=capiq_analystq_transcript_7 out=capiq_analystq_transcript_7b nodupkey;
by transcriptid transcriptpersonid;
run;
*1.58M obs;

**Create year and (calendar) quarter variables;
data capiq_analystq_transcript_8; set capiq_analystq_transcript_7b;
month = month(mostimportantdateutc);
year = year(mostimportantdateutc);
cal_qtr = qtr(mostimportantdateutc);	/* This can replace the data step below*/
run;

data capiqadj.capiq_analystq_transcript; set capiq_analystq_transcript8;
run;
* Dataset has person-event level word count and # of questions asked;

*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Part 2 Aggregate analyst-year-quarters
*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
/* Jared added below 4/14/21 and later*/
/*
proc sql;
	create table capiq_analystq_transcript_9 as select
	a.*, coalescec(b.companyname,c.companyname) as companyofperson
	from capiq_analystq_transcript_8 as a 
		left join capiq.ciqtranscriptperson as b on a.transcriptpersonid = b.transcriptpersonid
		left join db.wrds_professional as c on a.proid = c.proid;
quit;

proc sql;
	create table capiq_analystq_transcript_9b as select
	a.*,b.estimid
	from capiq_analystq_transcript_9 as a left join capiqadj.ciq_ibesbrokertranslation as b
	on a.companyofperson = b.companyofperson;
quit;
*/
proc sql;
	create table capiq_analystq_transcript_9 as select
	a.*, b.amaskcd
	from capiq_analystq_transcript_8 a left join capiqadj.ciq_ibesanalysttranslation b
	on a.transcriptpersonid = b.transcriptpersonid;
quit;

* For now use amaskcd for matching. May use estimid once we improve matching to incorporate CIQ brokers after 2012;
proc sort data = capiq_analystq_transcript_9 nodupkey; by transcriptid transcriptpersonid ;
where amaskcd ne "";
run;
* 1,577,365 obs down to 980,109 to ensure matching to IBES;
* 62% of observations have a matching amaskcd while 27.9% of transcriptpersonid's have a matching amaskcd;

*Number of events attended by analyst, # questions asked, etc.;
*Main unit of obs: analyst-quarter level;
*Main measures using (1) only earnings calls and (2) all events - or maybe just earnings, conferences, and AI Days;

* Count the number of events attended, questions asked, and sum the words spoken by each analyst-event type-calendar quarter;
proc sql;
	create table capiq_analystq as select distinct
	amaskcd, year, cal_qtr, keydeveventtypeid, count(transcriptpersonid) as nEvents, 
	sum(num_analystq_transcript) as nQuestions, 
	sum(wordcount_analystq_transcript) as nWordCount
	from capiq_analystq_transcript_9 group by amaskcd, keydeveventtypeid, year, cal_qtr;
quit;

proc sort data = capiq_analystq nodupkey; by amaskcd keydeveventtypeid year cal_qtr;
run;

data capiqadj.capiq_analyst_yrqtr; set capiq_analystq;
run;

data capiq_analystq; set capiqadj.capiq_analyst_yrqtr;
run;


*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
/**forecast accuracy quintile**/
*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

libname ibes "G:\My Drive\IBES datasets";
*Jared's
libname ibes "G:\My Drive\Research\RA Stuff\SAS\Data Files\IBES";

**Pull one-qtr ahead EPS forecasts; *-- Can I just change the 1 to 6 and keep everything else the same?;
data epsforecast_1; set ibes.det_epsus_20180426
	(keep = ticker estimator analys fpi value fpedats anndats anntims actual anndats_act);
	where fpi="6" /*and ticker in ("MSFT","HWP","SNO","APPL")*/;
	rename value=fcstEPS actual=actual_eps;
	datetime_eps = dhms(anndats, 0, 0, anntims);
	format datetime_eps datetime.;
run;
*4891k obs;

**Create forecast accuracy variable;
data epsforecast_1; set epsforecast_1;
	accuracy = -1 * abs(fcstEPS - actual_eps);
	where fcstEPS ne .;
run;
*NOTE: Multiply by negative one so greater values indicate higher accuracy;
*Since using firm-fiscal period to define accuracy quintiles, do not think we need to scale the accuracy variable;

*Drop obs with missing EA dates;
data epsforecast_2; set epsforecast_1;
	where anndats_act ne . and actual_eps ne .;
run;
*4430k obs;

*Drop obs where forecast date is after EA date;
data epsforecast_3; set epsforecast_2;
	where anndats lt anndats_act;
run;
*4421k obs;

**Sort analyst-firm-fiscal period forecasts to keep LAST forecast issued pre-EA;
proc sort data=epsforecast_3; 
	by ticker analys estimator fpedats descending datetime_eps;
run;

proc sort data=epsforecast_3 out=epsforecast_4 nodupkey; 
	by ticker analys /*estimator*/ fpedats;
run;
*1217k obs;

*Sort obs by firm-fiscal period;
proc sort data=epsforecast_4;
	by ticker fpedats datetime_eps analys /*estimator*/;
run;

*Count # analysts per firm-year;
proc sql;
	create table epsforecast_5
	as select *,
	count(unique analys) as n_analysts
	from epsforecast_4
	group by ticker, fpedats;
quit;

*Keep firm-qtr with at least five analysts (following Loh & Stulz);
data epsforecast_6; set epsforecast_5;
	where n_analysts ge 5;
run;
*1080k obs;

**Assign analysts into accuracy quintiles for each firm-qtr;
*Most accurate=0, Least accurate=4;
proc rank data=epsforecast_6 out=epsforecast_7 groups=5;
	by ticker fpedats;
	var accuracy;
	ranks accuracy_rank;
run;

proc sort data=epsforecast_7;
	by ticker fpedats descending accuracy_rank;
run;

* Set most accurate rank=5, least accurate rank=1;
data epsforecast_7; set epsforecast_7 ;
	accuracy_rank = (accuracy_rank + 1);
	year = year(anndats_act);
	cal_qtr = qtr(anndats_act);
run;

proc sql;
	create table analysts as select distinct
	analys*1 as amaskcd, year, cal_qtr, median(accuracy_rank) as median_accuracy_rank, count(ticker) as n_firms
	from epsforecast_7 group by analys, year, cal_qtr
	order by analys, year, cal_qtr;
quit;

data capiq_analystq2; set capiq_analystq;
amaskcd1 = input(amaskcd,6.);
run;

proc sql;
	create table capiq_analystq3 as select
	a.*, b.median_accuracy_rank, b.n_firms
	from capiq_analystq2 a left join analysts b
	on a.amaskcd1 = b.amaskcd
	and a.year = b.year
	and a.cal_qtr = b.cal_qtr;
quit;

proc means data = capiq_analystq3 n mean median;
var nEvents nQuestions nWordCount;
class median_accuracy_rank;
run;
* First forecast issued for the next quarter and last forecast issued for the next quarter

 * Aggregate Forecast Accuracy Quintile over the quarter and use the forecast accuracy










proc sort data=CapIQ.CIQtranscript out=test nodupkey;
by transcriptid;
run;

proc means data=test min p1 p5 p10 p25 p50 p75 p90 p95 p99 max;
var transcriptid;
run;


data transcript_person_notext_1; set CapIQ.wrds_transcript_person
(drop = componenttextpreview);
where transcriptid le 509094;
run;
*COMPLETED;

*Test download;
data CapIQ.transcript_person_notext_1; set work.transcript_person_notext_1;
run;


data transcript_person_notext_2; set CapIQ.wrds_transcript_person
(drop = componenttextpreview);
where transcriptid gt 509094 and transcriptid le 1085360;
run;
*COMPLETED;

data CapIQ.transcript_person_notext_2; set work.transcript_person_notext_2;
run;




data transcript_person_notext_3; set CapIQ.wrds_transcript_person
(drop = componenttextpreview);
where transcriptid gt 1085360 and transcriptid le 1662905;
run;
*COMPLETED;

data CapIQ.transcript_person_notext_3; set work.transcript_person_notext_3;
run;


data transcript_person_notext_4; set CapIQ.wrds_transcript_person
(drop = componenttextpreview);
where transcriptid gt 1662905 /*and transcriptid le 2154629*/;
run;
*COMPLETED;

data CapIQ.transcript_person_notext_4; set work.transcript_person_notext_4;
run;

















