* User inputs;
%let capIQ_path = G:\My Drive\Research\Data; /*define path to location of data*/

* Path to Capital IQ data;
/* 
    ciq library has capital IQ transcript data, wrds_gvkey, and wrds_professional if you do not have 
    access to wrds_professional, you may be able to perform a similar merge using only the transcript 
    data to populate missing companyofperson values
*/
libname ciq "&capIQ_path\capital IQ"; /* Can also modify to run this on WRDS */
libname adj "&capIQ_path\capital IQ\Adjusted";
/*
    library with ptgdet, recddet, and IBES-PERMNO link files from IBES 
*/
libname ibes "G:\My Drive\Research\SAS\Data Files\IBES"; 
%let iclink = iclink_20210121;

%let ciqfile = "&capIQ_path\capital IQ\Adjusted\ciqAFY_FmtdNms.csv";

%let ibesfile = "&capIQ_path\capital IQ\Adjusted\ibesAFY_FmtdNms.csv";

/*
    Put today's date to use at the end of main output files
*/
%let date = %sysfunc(today(), yymmddn8.);
