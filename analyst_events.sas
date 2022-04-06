*Last updated: January 9, 2021;
%let path = ""; /*define path to location of data*/
* Path to Capital IQ data;
libname ciq "&path\data\capitalIQ_raw"; /* Can also modify to run this on WRDS */
libname adj "&path\data\capitalIQ_adj";

*Count # obs in transcript_person dataset;
proc sql;
 select count(*) as N from ciq.wrds_transcript_person;
quit;


***Keep analyst question obs and drop character variables from transcript_person dataset;
**This piece takes a long time ~45 minutes to run on SAS (Unicode Support);
data work.capiq_analystquestions; set ciq.wrds_transcript_person
	(drop = transcriptcomponenttypename speakertypename transcriptpersonname companyofperson componenttextpreview);	
	where transcriptcomponenttypeid=3 and speakertypeid=3;
run;
*Note1: Need to use "Unicode Support" version of SAS for encoding reasons with characters;
*Note2: transcriptcomponenttypeid=3 is Question; 
*Note3: speakertypeid=3 is Analyst;
*Note4: Dropped all long character variables;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Part 1: Prepare analyst-event dataset;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

**Keep one obs per analyst per transcript;
proc sort data=capiq_analystquestions out=capiq_analystq_transcript_1 nodupkey;
by transcriptid transcriptpersonid;
quit;

*Drop question-specific variables;
data capiq_analystq_transcript_2; set capiq_analystq_transcript_1
(drop = transcriptcomponentid componentorder transcriptcomponenttypeid speakertypeid word_count);
run;
*Note: "proid" is "Unique identifier for a professional". How to use?;

***Keep FINAL transcripts with non-missing company id from transcript_detail dataset;
data capiq_events; set ciq.Wrds_transcript_detail;
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
	from capiq_analystq_transcript_5 as a left join ciq.WRDS_gvkey as b
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

data adj.capiq_analystq_transcript; set capiq_analystq_transcript8;
run;
* Dataset has person-event level observations;












