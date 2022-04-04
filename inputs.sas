* User inputs;
%let path = ""; /*define path to location of data*/
%let iclink = iclink_feb_2021;

* Path to Capital IQ data;
libname ciq "&path\data\capitalIQ_raw"; /* Can also modify to run this on WRDS */
libname adj "&path\data\capitalIQ_adj";
libname ibes "&path\data\ibes"; /* library with ptgdet, recddet, and IBES-PERMNO link files from IBES */

%let ciqfile = "&path\data\capitalIQ_adj\ciqAFY_FmtdNms.csv";

%let ibesfile = "&path\data\capitalIQ_adj\ibesAFY_FmtdNms.csv";
