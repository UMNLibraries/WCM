# WCM
A collection of python scripts and documentation related to quality control checks for records automatically imported into Alma from WorldShare Collection Manager.

These scripts and their purpose are more fully explained in the article by Kelly Thompson and Stacie Traill, "Leveraging Python to improve ebook metadata selection, ingest, and management", in the Code4Lib Journal, Issue 38, 2017-10-18, https://journal.code4lib.org/articles/12828.

## Rights and Reuse
This work is copyright (c) the Regents of the University of Minnesota, 2017.
It was created by Kelly Thompson and Stacie Traill.

## WCM-ftp-checks-new-DWM.py script
These scripts fetch files of records from the OCLC FTP server. The scipt is intended to be used AFTER the files have been imported into Alma via an auto-import profile which fetches from the FTP server. The scripts output reports on the records in the files (one report per file) and sorts them as either "fine" or as needing some type of further work (such as a check on whether the record is for a print resource instead of electronic, a check on non-English catalog records, and potential multi-volume/set records that may need manual URL clean-up).

## WCM-ftp-checks-updates-DWM.py script
These scripts fetch files of records form the OCLC FTP server and sort them BEFORE they are imported into Alma (either with an auto-import profile or manually as the case may be). The scripts sort out records into batches onto a local FTP server. These records are then automatically ingested by a scheduled Alma import profile, but problem records are added to a list for manual review to ensure that an OCLC number update was successful, and records with Knowledge Base URL changes which need manual resolution.
