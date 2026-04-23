* User inputs;
<<<<<<< HEAD
%let capIQ_path = /path/to/data; /*define path to location of data*/
=======
%let capIQ_path = ""; /*define path to location of data*/
>>>>>>> 7bf11d5a3a199908973942176f9b2aee8848b5ee

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
%let iclink = iclink_20210121; /*permno to ibes_ticker linking table;*/

%let ciqfile = "&capIQ_path\capital IQ\Adjusted\ciqAFY_FmtdNms_20240330.csv";

%let ibesfile = "&capIQ_path\capital IQ\Adjusted\ibesAFY_new_vint_FmtdNms_20240330.csv";

/*
    Put today's date to use at the end of main output files
*/
%let date = %sysfunc(today(), yymmddn8.);
