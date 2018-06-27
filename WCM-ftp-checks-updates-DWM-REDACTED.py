# This work is copyright (c) the Regents of the University of Minnesota, 2017.
# It was created by Kelly Thompson and Stacie Traill.

# -*- coding: utf-8 -*-
from ftplib import FTP
import re
import os
from os.path import splitext
import shutil
from pymarc import MARCReader, MARCWriter
from datetime import date
today = str(date.today())


'''
This script parses a single MARC binary file of Worldshare Collection Manager record updates into separate files based on the 
import profile they require (update bib only, update inventory only.) This is determined by data in the 960 field, which is 
removed after evaluation.
It also generates a list of records to be checked which 
indicate they have had an OCLC record number change. 
Creates one file per upload type per file delivered.

NOTE: Decided to keep 960 field in record output for now, because will allow for easier confirmation that the script
is working properly.  WCM merge rule does not add this field, so shouldn't get transferred into Alma.
909 is added in WCM record generation process.
'''
def fchange_sort(MARCfile, fname):
    
    '''
    Parses a MARC binary file based on 960 values into separate files for books, serials, maps, visual materials, and
    other formats. Output is one .mrc file for each format.
    '''
    #open a path to put the files for the FTP server - both OCN and BIB updates
    sorted_files_path = "C:/Users/kjthomps/Documents/WCM/file_fetching/updates/sorted_for_FTP " + today
    if not os.path.isdir(sorted_files_path):
        os.mkdir(sorted_files_path)
    
    #make a place to put the files with OCN updates for manual checking
    ocn_updates_path = "C:/Users/kjthomps/Documents/WCM/file_fetching/updates/OCN_updates_" + today
    if not os.path.isdir(ocn_updates_path):
        os.mkdir(ocn_updates_path)
        
    #make a place to put the files with URL updates for manual checking
    url_updates_path = "C:/Users/kjthomps/Documents/WCM/file_fetching/updates/URL_updates_" + today
    if not os.path.isdir(url_updates_path):
        os.mkdir(url_updates_path)
        

    fname_str = str(fname)
    print(fname)
    fname_str = fname_str.replace(".","")
    fname_str = fname_str.replace("mrc",".mrc")
    print(fname_str)
    fpref, fsuf = fname_str.split('.')
    print(fpref)
    print(fsuf)
    
    print(MARCfile)

    with open(MARCfile,'rb') as f:

        reader = MARCReader(f)
        
        # first, see if there are OCN or URL changes in the set; this will determine whether creating a file is necessary
        OCN_change_ct = 0
        URL_change_ct = 0
        writer_new = False
        writer_URLs = False
        
        for rec in reader:
            if rec['960']:
                field_960 = str(rec['960']['a'])
                if 'OCLC control number change' in field_960:
                    OCN_change_ct += 1
                if 'KB URL change' in field_960:
                    URL_change_ct += 1
        print("OCN_change_ct " ,OCN_change_ct)
        print("URL_change_ct ",OCN_change_ct)
        #if there are OCN updates or KB URL changes, create files to put those records in
    if OCN_change_ct > 0:
        writer_new_oclc_num_manual = MARCWriter(open(ocn_updates_path + "/" + fpref + '_new_oclc_num.mrc', 'wb'))
        writer_new = True
        print(writer_new)
    if URL_change_ct > 0:
        writer_update_URLs = MARCWriter(open(url_updates_path + "/" + fpref + '_update_URLs.mrc', 'wb'))
        writer_URLs = True
        print(writer_URLs)
        
    #create a file for all updates
    writer_update_bibs = MARCWriter(open(sorted_files_path + "/" + fpref + '_update_bibs.mrc', 'wb'))
    v = 0
    with open(MARCfile, 'rb') as f:
        reader = MARCReader(f)
        for rec in reader:
            v += 1
            print(v)
            if rec['960']:
                field_960 = str(rec['960']['a'])
                print(field_960)
                #writes record to correct file based on regex matches
                #these are ordered such that if a 960 field has more than one reason for the update, that the most critical to handle 
                #will be addressed first.  These are, in order: OCN change (affects matching), URL change, bib update.
                #Update: OCN changes can be processed alongside Bib updates.  URLs will need to be handled manually due to multi-vols?
                if 'OCLC control number change' in field_960:
                    writer_update_bibs.write(rec)
                    writer_new_oclc_num_manual.write(rec)
                    if 'KB URL change' in field_960:
                        writer_update_URLs.write(rec)
                elif 'KB URL change' in field_960:
                    writer_update_URLs.write(rec)
                    writer_update_bibs.write(rec)
                elif 'Subsequent record output' in field_960:
                    writer_update_bibs.write(rec)
                elif 'Master record variable field' in field_960:
                    writer_update_bibs.write(rec)
                else:
                    writer_update_bibs.write(rec)

    #closes master format files    
    writer_update_bibs.close()
    if writer_URLs == True:
        writer_update_URLs.close()
    if writer_new == True:
        writer_new_oclc_num_manual.close()

'''
CALLS BEGIN HERE
'''

'''
SEE WHAT FILES ARE ON THE FTP SERVER
'''

#create FTP instance with host and user
ftp = FTP('ftp2.oclc.org', '', '')
#ftp.login() --- not needed if credentials given when instance created

#working directory of files to be fetched
ftp.cwd('/metacoll')
ftp.cwd('out')
ftp.cwd('ongoing')
ftp.cwd('updates')

#return and save to array a list of file names on the server
flist = []
flist = ftp.nlst()
ftp.quit()

#compile the regular expressions for the file name patterns you will be looking for
regex = re.compile('^metacoll\.MNU\.updates\..*.mrc\Z')
regex2 = re.compile('^metacoll\.MNU\.merges\..*.mrc\Z')

fetchable_file_list = []

i = 0
for file in flist:
    
    match_test = regex.match(file)
    
    if match_test:
        fetchable_file_list.append(file)
    else:
        match_test2 = regex2.match(file)
        if match_test2:
            fetchable_file_list.append(file)
print(fetchable_file_list)

'''
GET A LIST OF THE OLD FILES
'''

path_old = "C:/Users/kjthomps/Documents/WCM/file_fetching/updates/old"
dir2 = os.listdir(path_old)
old_file_list = []

for file in dir2:
    file_name, extension = splitext(file)
    if ".mrc" not in file_name:
        file_name = file_name + ".mrc"
    old_file_list.append(file_name)
print(old_file_list)

'''
COMPARE THE LIST OF FILES ON THE SERVER WITH THE LIST OF OLD FILES TO SEE WHICH ONES ARE NEW
'''
files_to_evaluate = []

for file in fetchable_file_list:
    if file in old_file_list:
        print(file + " is old")
    else: 
        print(file + " is new")
        files_to_evaluate.append(file)
        
print(files_to_evaluate)

'''
PULL DOWN THE NEW FILES FROM THE FTP SERVER
'''

ftp = FTP('ftp2.oclc.org', '', '')
ftp.cwd('/metacoll')
ftp.cwd('out')
ftp.cwd('ongoing')
ftp.cwd('updates')

for file in files_to_evaluate:
    with open(str(file), 'wb') as p:
        ftp.retrbinary('RETR ' + file, p.write)

ftp.quit()
print('done fetching')

'''
PROCESS FILES YOU JUST FETCHED
'''

# Open a path
path = "C:/Users/kjthomps/Documents/WCM/file_fetching/updates"

#Get a directory of the files in that path
dirs = os.listdir( path )

# Make an empty list to put the file names in
#fetched_file_list = []

for fname in files_to_evaluate:
    record_in_hand = path + "/" + fname
    fchange_sort(record_in_hand, fname)
    print(record_in_hand)
    print(path_old)
    shutil.move(record_in_hand, path_old)

print("File processing complete.")