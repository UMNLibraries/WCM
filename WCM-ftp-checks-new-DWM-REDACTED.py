# This work is copyright (c) the Regents of the University of Minnesota, 2017.
# It was created by Kelly Thompson and Stacie Traill.


# -*- coding: utf-8 -*-
from ftplib import FTP
import re
import os
from os.path import splitext
import shutil
import csv
from pymarc import MARCReader
import pandas as pd
from datetime import date
today = str(date.today())

def record_dupes(MARCfile,fname):
    '''
    Parses a MARC binary file looking for duplicate record OCNs and
    writes them to a CSV file for analysis.
    '''
    #fname_str = str(MARCfile)
    fname = fname.replace(".","")
    
    with open(MARCfile, 'rb') as f:
        
        reader = MARCReader(f)
        
        #creates a folder to put files in
        datadir = 'check summaries ' + today
        if not os.path.isdir(datadir):
            os.mkdir(datadir)
        
        #iterate through each record and add the 001 value to a list
        OCN_list = []
        vol_list = []
        vol_count = 0   
        #read each record and evaluate for specific attributes
        for record in reader:
            
            #strip extra values out of the 001 field to get just the OCLC number
            ocn = str(record['001'])
            ocn = ocn.replace("=001  ","").replace("ocm","").replace("\\","")
            OCN_list.append(ocn)
            
            #look for an indication that the record might be for a multi-volume set or series
            desc = str(record['300'])
            search_term = 'vol'
            search_term2 = 'v.'

            #Should we add an evaluation to look for a volume indicator in the 020 field?

            if search_term in desc:
                is_vol = 'volume'
                vol_count = vol_count + 1
            elif search_term2 in desc:
                is_vol = 'volume'
                vol_count = vol_count + 1
            else:
                is_vol = ''
            vol_list.append(is_vol)
        
        record_count = len(OCN_list)                
        
    #Get count of unique and duplicate records in marc file
    seen = set()
    uniq = []
    dupes = []

    for x in OCN_list:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
        else:
            dupes.append(x)
            
    dupes_count = len(dupes)    
    unique_count = len(uniq)
    
    
    #create directories to sort your files into
    datadir_dupes = datadir + "/dupes or vols need work"
    if not os.path.isdir(datadir_dupes):
        os.mkdir(datadir_dupes)
        
    datadir_fine = datadir + "/fine"
    if not os.path.isdir(datadir_fine):
        os.mkdir(datadir_fine)
    
    #open a CSV files to put duplicates in        
    dupes_list = open(datadir + '/dupes-' + fname + '.csv', mode='w+', newline='', encoding='utf-8')
    dupes_wtr = csv.writer(dupes_list, delimiter=",")
    
    #Define the header row of your CSV file            
    count_row = ['Number of records', record_count]
    dupes_wtr.writerow(count_row)
    dupes_row = ['Number of duplicate OCNs', dupes_count]
    dupes_wtr.writerow(dupes_row)
    uniq_row = ['Number of unique OCNs', unique_count] 
    dupes_wtr.writerow(uniq_row)
    vol_row = ['Number of records with volume in description', vol_count]
    dupes_wtr.writerow(vol_row)
    blank_row = ['','']
    dupes_wtr.writerow(blank_row)
    
    #Create a pandas dataframe out of your 
    df = pd.DataFrame({'OCN':OCN_list, 'Volume':vol_list})
    df['Count'] = df.groupby(['OCN'])['OCN'].transform('count')        
    
    #Define the header row for the new CSV file for duplicate/volume identification    
    headerrow = ['001 OCN', 'count', 'is duplicate','has volume']
    dupes_wtr.writerow(headerrow)
    
    #get the output values for the CSV and write to the file
    i = 0
    for n in df.OCN:
        s = df.Count[i]
        if s > 1:
            t = 'duplicate'
        else:
            t = ''   
        v = df.Volume[i]
        row = [n,s,t,v]
        dupes_wtr.writerow(row)
        i = i + 1
    #close the duplicates/volumes CSV file
    dupes_list.close()
    
    #move the file into the appropriate folder depending on whether or not it needs manual handling or it's fine
    if dupes_count == 0:
        if vol_count != 0:
            shutil.move(datadir + '/dupes-' + fname + '.csv', str(datadir_dupes))
        else:
            shutil.move(datadir + '/dupes-' + fname + '.csv',str(datadir_fine))
    else:
        shutil.move(datadir + '/dupes-' + fname + '.csv', (datadir_dupes))
        

def check_quality(MARCfile, fname):
    '''
    Parses a MARC binary file looking for specific quality indicators and
    writes them to a CSV file for analysis.
    '''
    
    datadir = 'check summaries ' + today
    
    #fname_str = str(MARCfile)
    fname = fname.replace(".","")
    
    #create a place to put the analyses you're about to perform (log-type file)
    datadir_analyses = datadir + "/analyses"
    if not os.path.isdir(datadir_analyses):
        os.mkdir(datadir_analyses)
    
    #create a CSV file to write to    
    results = open(datadir_analyses + '/analysis-' + fname + '.csv', mode = 'w+', newline='', encoding='utf-8')
    result_wtr = csv.writer(results, delimiter=",")
    
    #define the header row for the CSV file & write it to the file
    headerrow = ['OCN','ELvl','040b','006pos0','006pos6','006pos9','007','008','300','337','338','050','650 or MeSH']
    result_wtr.writerow(headerrow)    
    
    #evaluate each record in the file
    with open(MARCfile, 'rb') as f:
        reader = MARCReader(f)
        OCN_list = []
        rec_level_list = []
        field040b_list = []
        field006_0_list = []
        field006_6_list = []
        field006_9_list = []
        field007_list = []
        field008_list = []
        field300_list = []
        #vol_list = []
        field337_list = []
        field338_list = []
        field050_list = []
        field650_list = []
    
        #iterate through each record looking for specific attributes, and add the results to lists
        for record in reader:
            #Get the OCLC number            
            if record['001']:
                ocn = str(record['001'])
                ocn = ocn.replace("=001  ","").replace("ocm","").replace("\\","")
            else:
                ocn = ''
            OCN_list.append(ocn)
            
            #Get the record encoding level value
            if record.leader:            
                rec_level = str(record.leader)
                rec_level = rec_level[17:18]
            else:
                rec_level = 'null'
            rec_level_list.append(rec_level)
            
            #Get the language of cataloging abbreviation from the 040 field
            if record['040']:            
                f040b = record['040']
                f040b = str(f040b['b'])
            else:
                f040b = ''
            field040b_list.append(f040b)
            
            #Get the values from the 006 field that hint at whether the record is for a print or electronic resource
            if record['006']:
                #Position 00 - Form of item: m = computer file / electronic resource
                f006_0 = str(record['006'])
                f006_0 = f006_0[6:7]
                #Position 06 - Form of item: o = online
                f006_6 = str(record['006'])
                f006_6 = f006_6[12:13]
                #Position 09 - Type of computer file: d = document
                f006_9 = str(record['006'])
                f006_9 = f006_9[15:16]
                
            else:
                f006_0 = ''
                f006_6 = ''
                f006_9 = ''
                
            field006_0_list.append(f006_0)
            field006_6_list.append(f006_6)
            field006_9_list.append(f006_9)
            
            #Get the values from the 007 field that hint at whether the record is for a print or electronic resource
            #007 pos 0: c = electronic resource
            #007 pos 1: r = remote
            if record['007']:
                f007 = str(record['007'])
                f007 = f007[6:8]
            else:
                f007 = ''
            field007_list.append(f007)
            
            #Get the value of 008 character 23 (0 = online resource)
            if record['008']:
                f008 = str(record['008'])
                f008 = f008[29:30]
            else:
                f008 = ''
            field008_list.append(f008)
            
            #Get the value of 300 subfield a (physical description - extent)
            if record['300']:            
                f300 = record['300']
                f300 = str(f300['a'])
                f300 = f300[0:17]
                field300_list.append(f300)
            else:
                f300 = ''
                field300_list.append(f300)
            
            '''
            f300_vol = str(record['300'])
            search_string = 'vol'
            vol_boolean = search_string in f300_vol
            vol_list.append(vol_boolean)
            '''
            
            if record['337']:
                f337 = record['337']
                f337 = (f337['a'])
                field337_list.append(f337)
            else:
                field337_list.append('')
            
            if record['338']:
                f338 = record['338']
                f338 = f338['a']
                field338_list.append(f338)
            else:
                field338_list.append('')
            #is there an LC call number?    
            if record['050']:
                field050_list.append('050')
            else:
                field050_list.append('')
                
            #find out if there are LCSH or MeSH subject headings
            if record['650']:
                f650 = record['650']
                if (f650.indicator2 == '0'):
                    f650 = 'LCSH'
                elif (f650.indicator2 == '2'):
                    f650 = 'MeSH'
                else:
                    f650 = ''
            else:
                f650 = ''
            field650_list.append(f650)
    #Define the list of all the fields you just evaluated.  Should correspond with your header row, too.        
    eval_list = [OCN_list,rec_level_list,field040b_list,field006_0_list,field006_6_list,field006_9_list,field007_list,field008_list,field300_list,field337_list,field338_list,field050_list,field650_list]        
    
    #Iterate through all your lists by index, pulling the values record by record and write to file  
    i = 0
    for n in OCN_list:
        row = []
        for m in eval_list:
            a = m[i]
            row.append(a)
        i = i + 1
        
        result_wtr.writerow(row)
    #close the file
    results.close()
    
    #create directories to sort your files into for manual processing
    datadir_noneng = datadir + "/non-eng"
    if not os.path.isdir(datadir_noneng):
        os.mkdir(datadir_noneng)
    
    datadir_printrec = datadir + "/print_record"
    if not os.path.isdir(datadir_printrec):
        os.mkdir(datadir_printrec)    
    
    '''
    The next bit here goes through all those data values you just pulled out of the MARC data and compares them to a list of what we would expect to see in a bib record for an 
    electronic resource, with proper subject analysis, with language of cataloging english.  Binary scores are assigned for each field value, and these are talled into a composite
    score for each record, and used to flag records that may be non-english language of cataloging, a record for a print resource.  It also flags for non-RDA records and records w/o
    subject/class access, but those records are not sorted out for manual processing.
    '''    
    
    scores = open(datadir + '/scores-' + fname + '.csv', mode = 'w+', newline='', encoding='utf-8')
    score_wtr = csv.writer(scores, delimiter=",")
    
    headerrow = ['OCN','EnLvl','cat_lang','comp_file_elec_res','online_006','document','categ_e_res_remote','online_008','item_desc','rda_media_type','rda_carrier_type', 'LC_call_no', 'LCSH or MeSH', 'total', 'percent','non-eng lang?','print record?','rda record?','has class?']
    score_wtr.writerow(headerrow)
        
    i = 0
    non_eng_lang_count = 0
    print_rec_count = 0
    
    field_list = [field040b_list,field006_0_list,field006_6_list,field006_9_list,field007_list,field008_list,field300_list,field337_list,field338_list,field050_list]
    #create counter for noneng, print records
    #''    
    
    for n in OCN_list:
        row = []
        #The test_list is the list of values we have pre-defined as the desireable value for the given ebook records.
        test_list = ['eng','m','o','d','cr','o','1 online resource','computer','online resource','050']
        
        #print the OCN as a key value
        OCN_key = OCN_list[i]
        row.append(OCN_key)
        
        EnLvl = rec_level_list[i]
        row.append(EnLvl)
        
        c = 0
        #Check each parameter against the desired value        
        for t in test_list:
            param = field_list[c]
            if(param[i] == t):
                test_bool = 1
            else:
                test_bool = 0
            row.append(test_bool)
            c = c + 1
        if (field650_list[i] == 'LCSH'):
            subjhead = 1
        elif (field650_list[i] == 'MeSH'):
            subjhead = 1
        else:
            subjhead = 0
        row.append(subjhead)
        #calculate basic summary statistics
        total = sum(row[2:13])
        percent = total / 11 
        percent = '{:.1%}'.format(percent)
        row.append(total)
        row.append(percent)
        
        #Flag if english not listed as language of cataloging
        if (row[2] == 0):
            row.append('non-eng lang?')
            non_eng_lang_count = non_eng_lang_count + 1
        else: row.append('')
        
        #Flag as possibly for a print resource if (5/6)+ of the electronic resource indicators are not as expected
        is_print_rec = row[3] + row[4] + row[5] + row[6] + row[7] + row[8]
        if (is_print_rec < 4):
            row.append('print record?')
            print_rec_count = print_rec_count + 1
        else: row.append('')
        
        #Flag if the record probably isn't an RDA record
        is_rda_rec = row[9] + row[10]
        if (is_rda_rec < 1):
            row.append('rda record?')
        else: row.append('')
        
        #Flag if the record is missing both a call number and subject headings
        has_class = row[11] + row[12]
        if (has_class < 1):
            row.append('no class?')
        else: row.append('')    
            
        score_wtr.writerow(row)    
        i = i + 1
    '''    
    summary_row = []
    count = len(OCN_list)
    summary_row.append(count)
    i = 0
    for e in field_list:
        b = field_list[i]
        count = sum(b)
    '''    
    
    scores.close()
    
    datadir_fine = datadir + "/fine"
    
    scores_path =  datadir + '/scores-' + fname + '.csv'
    
    #Sort non-english language and print records off into their own folder for manual processing.  Put everything else in a "fine" folder to be ignored.
    if non_eng_lang_count > 0:
        shutil.copy2(scores_path, datadir_noneng)
        if print_rec_count > 0:
            shutil.copy2(scores_path, datadir_printrec)
        else:
            os.remove(scores_path)
    elif print_rec_count > 0:
        shutil.move(scores_path, datadir_printrec)
    else:
        shutil.move(scores_path, datadir_fine)
'''
def summarize_quality(MARCfile):
    
    #reports out summary statistics for the file
    
    datadir = 'check summaries'
    if not os.path.isdir(datadir):
        os.mkdir(datadir)
        
    summarize_qual = open(datadir + '/summary.csv', mode = 'w+')
    summarize_wtr = csv.writer(summarize_qual, delimiter=",")
    
    headerrow = ['040b','006pos0','006pos6','006pos9','007','008','300','337','338','050','650']
    summarize_wtr.writerow(headerrow)  
    
    df = pd.read_csv('check summaries/scores.csv')
    row = []
    column_list = df.columns.values
    column_list = column_list[2::]
    column_list = column_list[2::10]
    for s in column_list:
        average = np.mean(df[s])
        average = '{:.1%}'.format(average)
        row.append(average)
    summarize_wtr.writerow(row)
    
    q = pd.Series(df['EnLvl'])
    q = q.value_counts()
    summarize_wtr.writerow(q)
            
    summarize_qual.close()
 '''   
'''def main():
   
    Main function: gets user input for unparsed MARC file and runs
    other functions
   

    record_dupes(fname)
    check_quality(fname)
    summarize_quality(fname)
    
    print("File processing complete.")
    
main()'''


'''
CALLS BEGIN HERE
'''

'''
FETCH FILES FROM THE FTP SERVER
'''
#create FTP instance with host and user
ftp = FTP('ftp2.oclc.org', '', '')
#ftp.login() --- not needed if credentials given when instance created

#working directory of files to be fetched
ftp.cwd('/metacoll')
ftp.cwd('out')
ftp.cwd('ongoing')
ftp.cwd('new')

#return and save to array a list of file names on the server
flist = []
flist = ftp.nlst()

#compile the regular expressions for the file name patterns you will be looking for
#note: for most collections, the file name pattern regex can be the same in Alma and this script,
#however, for the ACS Symposium files, the regex from Alma DID NOT work in this script.  Test any
#new collections added to this list to ensure this isn't the case for any others, too.
'''
ACS = re.compile('^metacoll\.MNU\.new\.*\..*\.acssymp.*\.1\.mrc\Z')
bloompopmus = re.compile('^metacoll\.MNU\.new\.*\..*\.bloompopmus.*.mrc\Z')
#chemlibnetbase = re.compile('^metacoll\.MNU\.new\.*\..*\.chemlibnetbase\.1\.mrc\Z')
#engnetbase = re.compile('^metacoll\.MNU\.new\.*\..*\.engnetbase\.1\.mrc\Z')
#environetbase = re.compile('^metacoll\.MNU\.new\.*\..*\.environetbase.1.mrc\Z')
#mathnetbase = re.compile('^metacoll\.MNU\.new\.*\..*\.MATHnetBASE.1.mrc\Z')
clinicalkey = re.compile('^metacoll\.MNU\.new\.*\..*\.clinicalkey.*.mrc\Z')
cqpresswid = re.compile('^metacoll\.MNU\.new\.*\..*\.cqpresswid.*.mrc\Z')
scidirall = re.compile('^metacoll\.MNU\.new\.*\..*\.scidirall*.1.mrc\Z')
ieeewiley = re.compile('^metacoll\.MNU\.new\.*\..*\.ieeewiley.1.mrc\Z')
iopebooks = re.compile('^metacoll\.MNU\.new\.*\..*\.iopebooks.1.mrc\Z')
knovel = re.compile('^metacoll\.MNU\.new\.*\.knovel.*\.mrc\Z')
llmc = re.compile('^metacoll\.MNU\.new\.*\..*\.llmc\.1\.mrc\Z')
lwwpharm = re.compile('^metacoll\.MNU\.new\.*\..*\.lwwpharm.*.mrc\Z')
mcghill = re.compile('^metacoll\.MNU\.new\.*\..*\.mcghill.*\.mrc\Z')
#metopera = re.compile('^metacoll\.MNU\.new\.*\..*\.MetOpera.1.mrc\Z')
napmonos = re.compile('^metacoll\.MNU\.new\.*\..*\.napmonos\.1\.mrc\Z')
ovidipc = re.compile('^metacoll\.MNU\.new\.*\..*\.ovidipc.1.mrc\Z')
oxfordref = re.compile('^metacoll\.MNU\.new\.*\..*\.oxrefp.1.mrc\Z')
sageref = re.compile('^metacoll\.MNU\.new\.*\..*\.sageKR.*\..*\.mrc\Z')
springerbtaa = re.compile('^metacoll\.MNU\.new\.*\..*\.springerbtaa20.*\..*\.mrc\Z')
springerengint = re.compile('^metacoll\.MNU\.new\.*\..*\.springerengint\..*\.mrc\Z')
statref = re.compile('^metacoll\.MNU\.new\.*\..*\.statref\..*\.mrc\Z')
wageningenap = re.compile('^metacoll\.MNU\.new\.*\..*\.wageningenap\.1\.mrc\Z')
wiley = re.compile('^metacoll\.MNU\.new\.*\..*\.wiley2017.1.mrc\Z')

coll_list = [ACS,bloompopmus,clinicalkey,cqpresswid,scidirall,ieeewiley,iopebooks,knovel,llmc,lwwpharm, mcghill,napmonos,ovidipc,oxfordref,sageref,springerbtaa,springerengint,statref,wageningenap,wiley]

i = 0
for file in flist:
    for n in coll_list:
        match_test = n.match(file)
    
        if match_test:
            with open(str(file) + '.mrc', 'wb') as p:
                ftp.retrbinary('RETR ' + file, p.write)
'''

regex = re.compile('^metacoll\.MNU\.new\..*.mrc\Z')

i = 0
for file in flist:
    match_test = regex.match(file)
    
    if match_test:
        with open(str(file) + '.mrc', 'wb') as p:
            ftp.retrbinary('RETR ' + file, p.write)

ftp.quit()
print('done fetching')


'''
PROCESS FILES YOU JUST FETCHED
'''

# Open a path
path = "C:/Users/kjthomps/Documents/WCM/file_fetching/new"

#Get a directory of the files in that path
dirs = os.listdir( path )

# Make an empty list to put the file names in
fetched_file_list = []

#Add file names to the list if they have a .mrc extension
for file in dirs:
    file_name,extension = splitext(file)
    if extension == ".mrc":
        fetched_file_list.append(file)

print(fetched_file_list)

#Do the same thing again, but this time with the folder of files that have already been processed before
path2 = "C:/Users/kjthomps/Documents/WCM/file_fetching/new/old"
if not os.path.isdir(path2):
    os.mkdir(path2)
dir2 = os.listdir(path2)

old_file_list = []

for file in dir2:
    file_name,extension = splitext(file)
    old_file_list.append(file)

#All of the printed output here is just FYI for if you're running the script in a console.  I prefer to use an IPython console in the Spyder IDE when I'm testing/de-bugging, but normally
#I'm running my scripts automatically with a task scheduler so they aren't necessary and can be commented out.
print(old_file_list)

#Look for the new file name values in the list of old file names
files_to_evaluate = []

for f in fetched_file_list:
    if f in old_file_list:
        #delete the file
        print(f + " is old")
        os.remove(path + "/" + f)
    else:
        print(f + " is new")
        files_to_evaluate.append(f)
        
print(files_to_evaluate)

#Here is where you call most of the functions defined above, and move the files out of the working directory so it's cleared out and the script knows not to process them again next time.
for fname in files_to_evaluate:
    record_in_hand = path + "/" + fname
    record_dupes(record_in_hand, fname)
    check_quality(record_in_hand, fname)
    shutil.move(record_in_hand,path2)

print("File processing complete.")