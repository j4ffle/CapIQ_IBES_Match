# CapIQ_IBES_Match

## Create translation files between capital IQ conference call participants and IBES analysts

To create ciq_ibesbrokertranslation and ciq_ibesanalysttranslation files:

1. Create a unique set of analyst-broker-firm-year observations from the combination of the price target and recommendation detail files from IBES that pre-date the significant changes (anonymization and removal of individual analyst data) made in October 2018. The variables remaining in this dataset are ticker (firm ID), estimid (broker ID), analyst (analyst name), year, permno, gvkey, Input datasets (IBES: ptgdet, recddet)
    a. Combine price target and recommendation detail files in a full join on ticker, amaskcd, estimid, and year.
    b. Reduce to unique analyst-broker-firm-year observations.
    c. Remove observations where analyst = “RESEARCH DEPARMEN”
2. Create a unique set of analyst-broker-firm-year observations from the Capital IQ transcripts dataset. Input datasets (CIQ: wrds_transcript_person, wrds_transcript_detail, wrds_gvkey ciqtranscriptperson, wrds_professional)
    a. Prepare transcript-analyst level copy of transcript data and keep all observations where the transcriptcomponenttypeid = 3 (questions) and the speakertypeid = 3 (analysts).
    b. Keep transcripts (keydeveventtypeid) related to Earnings Calls (48), Guidance/Update Calls (49), Shareholder/Analyst Calls (50), Conference Presentation Calls (51), M&A Calls (52), and Analyst/Investor Day (192).
    c. Merge in GVKEY from wrds_gvkey (on companyid) file and person and companyofperson names from the ciqtranscriptperson (on transcriptpersonid) and wrds_professional (on proid).
    d. Reduce to unique analyst-broker-firm-year observations.
    e. Remove observations where transcriptpersonname is any of "Unknown Analyst", "Unkown Analyst", or "Unidentified Audience Member".
3. Extract and standardize the last name from both IBES and CIQ datasets in Python (CIQ_IBES_PrepMerge.py).
    a. Remove suffixes such as Jr, III, CFA, etc.
    b. Remove non-standard characters.
    c. Remove spaces and hyphens for analysts with two last names.
4. Merge CIQ and IBES datasets on analyst (amaskcd), firm (gvkey), and year
    a. Keep only the observations with a proper match.
    b. Create a broker matched dataset by reducing the merged dataset to unique companyofperson-estimid matches.+
    c. Create an analyst matched dataset by reducing the merged dataset to unique transcriptpersonid-amaskcd matches.
    d. Due to analysts’ switching firms and brokerage mergers, some companyofperson names have multiple estimid matches. To remove these incorrect matches, we count all the individual analyst-firm-year matches used to compose the companyofperson-estimid match and keep the pair with the greatest number of matches.
    e. Due to non-standardized companyofperson names prior to 2012 in the CIQ database, many broker names are used only a handful of times and/or are misspelled and at times, this may lead to an incorrect match. We sum the total number of matches under each remaining companyofperson by estimid and produce the percent of the matches for that estimid contributed by that companyofperson and keep those matches comprising greater than 1% of that estimid’s matches.
