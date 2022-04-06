*Read in user inputs;
%include "C:\Users\flakej\Dropbox\GitHub\CapIQ_IBES_Match\inputs.sas";
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
*Step 1: Get analyst-firm-years from Capital IQ;
* - Start with analyst-event level dataset ;
* - Merge in transcriptpersonname and companyname (companyofperson);
* - Export transcriptpersonid,gvkey, year observations to Python to 
		format last name to merge with IBES;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

*Count # obs in transcript_person dataset and print to output;
proc sql;
 select count(*) as N from ciq.wrds_transcript_person;
quit;


***Keep analyst question obs and drop character variables from transcript_person dataset;
**This piece takes a long time ~45 minutes to run on SAS (Unicode Support);
data work.capiq_analystquestions; set ciq.wrds_transcript_person
	(keep = transcriptid transcriptpersonid proid transcriptcomponenttypeid speakertypeid);	
	where transcriptcomponenttypeid=3 and speakertypeid=3;
run;
*Note1: Need to use "Unicode Support" version of SAS for encoding reasons with characters;
*Note2: transcriptcomponenttypeid=3 is Question; 
*Note3: speakertypeid=3 is Analyst;
*Note4: Dropped all long character variables;

* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* Part 1: Prepare analyst-firm-year dataset;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;
* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

**Keep one obs per analyst per transcript;
*Drop question-specific variables;
proc sort data=capiq_analystquestions out=capiq_analyst_transcript_1
(keep = transcriptid transcriptpersonid proid) nodupkey;
by transcriptid transcriptpersonid;
quit;
*Note: "proid" is "Unique identifier for a professional". How to use?;

***Keep FINAL transcripts with non-missing company id from transcript_detail dataset;
**Keep relevant event types (see below) - may not be necessary;
data capiq_events; set ciq.Wrds_transcript_detail;
	where companyid ne . and transcriptpresentationtypename = "Final" and 
    keydeveventtypeid in (48,49,50,51,52,192);
	year = year(mostimportantdateutc);
run;
*890k obs;
*Keeping Earnings Calls (48), Guidance/Update Calls (49), Shareholder/Analyst Calls (50),
         Conference Presentation Calls (51), M&A Calls (52), and Analyst/Investor Day (192);
*850k obs;

**Keep one obs per keydevid (duplicates for different versions of same transcript - e.g., edited, proofed, audited);

*Perform sort keeping newest transcript per event on top;
proc sort data=capiq_events; 
by companyid keydevid descending transcriptcreationdate_utc descending transcriptcreationtime_utc;
run;
*CapIQ guide says "When in doubt, the version with the most recent time stamp is the most accurate";
*Note: I think sorting on "descending transcriptid" instead of time stamps would also work;

*Drop duplicate transcripts for same event;
proc sort data=capiq_events out=capiq_events_1 nodupkey;
by companyid keydevid;
run;
*313k obs;

**Keep relevant variables;
data capiq_events_2; set capiq_events_1
(keep = companyid transcriptid year mostimportantdateutc);
run;
*Check for duplicates?;

* Merge in gvkey ;
proc sql;
	create table capiq_events_3 as select distinct 
	a.*, b.gvkey
	from capiq_events_2 as a left join ciq.WRDS_gvkey as b
	on a.companyid = b.companyid
	and (year(b.startdate) le a.year or b.startdate = .B)
	and (year(a.mostimportantdateutc) le year(b.enddate) or b.enddate = .E);
quit;

data capiq_events_3b; set capiq_events_3;
where gvkey ne " ";
run;

***Merge events data with analyst question data;
**Perform merge;
proc sql;
	create table capiq_analyst_transcript_2 as select distinct 
	a.*, b.companyid, b.gvkey, b.year
	from capiq_analyst_transcript_1 as a inner join capiq_events_3b as b
	on a.transcriptid = b.transcriptid;
quit;

* Go to unique analyst-transcript obs;
proc sort data=capiq_analyst_transcript_2 out=capiq_analyst_transcript_3 nodupkey;
by transcriptid transcriptpersonid;
quit;
*1.58 million obs;

* Merge in analyst name and analyst's company name;
proc sql;
	create table ciqafy as select distinct
	a.*, b.transcriptpersonname as transcriptpersonname, coalescec(b.companyname,c.companyname) as companyofperson
	from capiq_analyst_transcript_3 as a 
		left join ciq.ciqtranscriptperson as b on a.transcriptpersonid = b.transcriptpersonid
		left join ciq.wrds_professional as c on a.proid = c.proid;
quit;

* Go from analyst-event level data to analyst-gvkey-year;
proc sort data=ciqafy out=ciqafy2 (drop = transcriptid) nodupkey;
by gvkey transcriptpersonid companyofperson year;
quit;

* Save dataset with unique analyst-firm-year obsercations from Capital IQ;
data adj.CIQAnalystFirmYear; set ciqAFY2;
where transcriptpersonname not in ("Unknown Analyst","Unkown Analyst","Unidentified Audience Member");
run;
* 794k analyst, firm, years;

* Unique transcriptpersonid's;
proc sort data = ciqAFY2 out = CIQAnalysts (
keep = transcriptpersonid transcriptpersonname companyofperson) nodupkey; 
by transcriptpersonid companyofperson;
run;

data adj.CIQAnalysts; set CIQAnalysts;
where transcriptpersonname not in ("Unknown Analyst","Unidentified Audience Member");
run;
* 112,764 unique transcriptpersonid's;

* Unique companyofperson's;
proc sort data = ciqAFY2 out = CIQBrokers (
keep = companyofperson) nodupkey; 
by companyofperson;
run;

data adj.CIQBrokers; set CIQBrokers;
where companyofperson ne "";
run;
* 16,399 unique companyofperson;