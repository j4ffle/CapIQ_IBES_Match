""" readsas reads a sas data file into a pandas dataframe
    and converts the variable names to all lower case strings. 
    For columns imported as byte objects, it first attempts
    to convert byte objects into literal strings using 'utf-8'
    and if it fails it uses 'latin-1' encoding. It replaces byte 
    object columns with decoded literal strings.

    sas2stata performs the steps as described above and then 
    converts the pandas dataframe to Stata datafile (.dta) with
    the variable names converted to upper case strings as labels.
"""
import pandas as pd
import numpy as np
import pyreadstat
def sas2stata(sasfile,statafile):
    """ sas2sstata reads a sas data file into a pandas dataframe 
    and converts the variable names to all lower case strings. 
    For columns imported as byte objects, it first attempts to 
    convert byte objects into literal strings using 'utf-8' and 
    if it fails it uses 'latin-1' encoding. It replaces byte object
    columns with decoded literal strings and converts the pandas
    dataframe to Stata datafile (.dta) with the variable names
    converted to upper case strings as labels.
    """
    dat = pd.read_sas(sasfile)
    dat.columns = map(str.lower,dat.columns)
    str_df = dat.select_dtypes([np.object])
    if str_df.empty == False:
        try:
            str_df = str_df.stack().str.decode('utf-8').unstack()
        except:
            str_df = str_df.stack().str.decode('latin-1').unstack()
    for col in str_df:
        dat[col] = str_df[col]
    # Read in SAS dataset, make all variable names lower case and change decoding type if latin decoding doesn't work
    varlabels = sorted(set(map(str.upper,dat.columns)))
    varlist = sorted(set(dat.columns))
    labels = dict()
    for key in varlist:
        for value in varlabels:
            if key.lower() == value.lower():
                labels[key]=value
    # create uppercase labels for all variable names
    dat.to_stata(statafile, write_index=False, variable_labels = labels)
    # Transfer dataset to Stata
    
    
def readsas(sasfile):
    """ readsas reads a sas data file into a pandas dataframe and
    converts the variable names to all lower case strings. 
    For columns imported as byte objects, it first attempts to 
    convert byte objects into literal strings using 'utf-8' and if
    it fails it uses 'latin-1' encoding. It replaces byte object
    columns with decoded literal strings.
    """
    dat = pd.read_sas(sasfile)
    dat.columns = map(str.lower,dat.columns)
    str_df = dat.select_dtypes([np.object])
    if str_df.empty == False:
        try:
            str_df = str_df.stack().str.decode('utf-8').unstack()
            print("Used UTF-8 to decode {}".format(sasfile))
            enc = 'utf-8'
        except:
            str_df = str_df.stack().str.decode('latin-1').unstack()
            print("Used latin-1 to decode {}".format(sasfile))
            enc = 'latin-1'
    for col in str_df:
        dat[col] = str_df[col]
    return dat

def sas2csv(sasfile,csvfile,index_out=False):
    """ readsas reads a sas data file into a pandas dataframe and
    converts the variable names to all lower case strings. 
    For columns imported as byte objects, it first attempts to 
    convert byte objects into literal strings using 'utf-8' and if
    it fails it uses 'latin-1' encoding. It replaces byte object
    columns with decoded literal strings.
    """
    dat = pd.read_sas(sasfile)
    dat.columns = map(str.lower,dat.columns)
    str_df = dat.select_dtypes([np.object])
    if str_df.empty == False:
        try:
            str_df = str_df.stack().str.decode('utf-8').unstack()
            print("Used UTF-8 to decode {}".format(sasfile))
            enc = 'utf-8'
        except:
            str_df = str_df.stack().str.decode('latin-1').unstack()
            print("Used latin-1 to decode {}".format(sasfile))
            enc = 'latin-1'
    for col in str_df:
        dat[col] = str_df[col]
    dat.to_csv(csvfile,index=index_out,encoding = enc)