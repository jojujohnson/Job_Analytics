#!/usr/bin/env python3
"""
  FileName : FileParser.py
  Version  : v0.0.0.1
  Details  : Script connects to predefined AWS S3 Bucket and reads file content. Script Supported File Type : PDF, DOC, DOCX.
"""
import configparser
import os
import logging
import boto3
import docx2txt
import PyPDF2
import pdfminer
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import StringIO
from io import BytesIO

#
print('File Parsing Program : Executing')

logging.basicConfig(level = logging.INFO, filemode = 'w', filename = 'FileParser')

os.chdir('/fileparser')
print('Home Path :' + os.environ['HOME'])
#os.chdir(os.environ['HOME'])
Working_Dir=os.environ['HOME']
print('Working Directory :' + Working_Dir + '\n')
logging.info('Working Directory :' + Working_Dir + '\n')

config = configparser.ConfigParser()
config.read(Working_Dir+'/AWS_CONFIG.cfg')
os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS_CREDENTIALS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS_CREDENTIALS", "AWS_SECRET_ACCESS_KEY")
bucket_name = config.get("S3", "bucket_name")
s3_bucket = config.get("S3", "s3_bucket")
bucket_name="fileparser"
"""
  Declare list for various file format
"""
pdf_file_list = []
doc_file_list = []
audio_video_file_list = []
powerpoint_file_list = []
other_format_file_list = []
unavailable_format_file_list = []
content_not_readable_file_list = []

"""
  Declare  variables 
"""
filecount=0
parsed_filecount=0
other_filecount=0
#
print("S3 Bucket Name : "+ bucket_name) 

os.chdir('/fileparser')
Working_Dir=os.getcwd()
print('Working Directory :' + Working_Dir + '\n')
logging.info('Working Directory :' + Working_Dir + '\n')
      
"""
  Read files available in S3 Bucket hosted via mount point. 
"""
for root,directory_names,file_name_list in os.walk(os.getcwd()):
  for filename in file_name_list:
    print("\n##########################\nProcessing Record : " + filename)
    logging.info("\n##########################\nProcessing Record : " + filename)
    filecount=filecount+1
    FullFilePath=os.path.join(root, filename)

    if os.path.isfile(FullFilePath):
      if filename.find('.pdf') > 0:
       print("File Type : PDF")
       logging.info("File Type : PDF")
       
       pdfFileObj = open(FullFilePath,'rb')
       pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
       #final_file_totalpages=pdfReader.numPagesreading
       #print(final_file_totalpages)
       pageObj = pdfReader.getPage(0) 
       FileContent=pageObj.extractText()

       filesize=os.path.getsize(FullFilePath)
       filesize=str(filesize/1024/1024)

       pdfFileObj.close()

       if(FileContent.strip()==''):
        print('Retry With PDFMiner')
        logging.info('Retry With PDFMiner')
        
        pagenums = set()
        output = StringIO()
        manager = PDFResourceManager()
        converter = TextConverter(manager, output, laparams=LAParams())
        interpreter = PDFPageInterpreter(manager, converter)
    
        infile = open(FullFilePath, 'rb') #open the file for read in binary mode
    
        for page in PDFPage.get_pages(infile, pagenums):     #iterate with the pdf interpreter through the pdf pages
         interpreter.process_page(page)
    
        #Close the files and converters
        infile.close()
        converter.close()
        FileContent= output.getvalue()
        output.close()

        
       """
      	Display Processed Details 
       """    
	  
       if(FileContent.strip()==''):
        content_not_readable_file_list.append(FullFilePath)
        print("Result : Unable to Read File Content")
        logging.info("Result : Unable to Read File Content")
       else:
        remove_line_breaks = FileContent.replace("\n", "")
        remove_line_breaks = remove_line_breaks.replace("\t", "")
        final_file_content = remove_line_breaks.replace("  ", "")
        pdf_file_list.append(filename)
        parsed_filecount=parsed_filecount+1
        print("File Name : "+ filename)
        print("File Word Count : "+ str(len(final_file_content.strip())))
        print("File Size : "+ filesize +" MB")
        print("File Full Path and Name : "+ FullFilePath) 
        print("File Content : \n"+ final_file_content)
        logging.info("File Name : "+ filename)
        logging.info("File Word Count : "+ str(len(final_file_content.strip())))
        logging.info("File Size : "+ filesize +" MB")
        logging.info("File Full Path and Name : "+ FullFilePath) 
        logging.info("File Content : \n"+ final_file_content)

      elif filename.find('.doc') > 0:
       print("File Type : DOC")
       logging.info("File Type : DOC")

       FileContent = docx2txt.process(FullFilePath)
       filesize=os.path.getsize(FullFilePath)
       filesize=str(filesize/1024/1024)  
    
       """
        Display Processed Details 
       """ 
       if(FileContent.strip()==''):
        content_not_readable_file_list.append(FullFilePath)
        print("Result : Unable to Read File Content")
        logging.info("Result : Unable to Read File Content")
       else:
        remove_line_breaks = FileContent.replace("\n", " ")
        remove_line_breaks = remove_line_breaks.replace("\t", " ")
        final_file_content = remove_line_breaks.replace("  ", " ")
        doc_file_list.append(filename)
        parsed_filecount=parsed_filecount+1
        print("File Name : "+ filename)
        print("File Word Count : "+ str(len(final_file_content.strip())))
        print("File Size : "+ filesize +" MB")
        print("File Full Path and Name : "+ FullFilePath) 
        print("File Content : \n"+ final_file_content)
        logging.info("File Name : "+ filename)
        logging.info("File Word Count : "+ str(len(final_file_content.strip())))
        logging.info("File Size : "+ filesize +" MB")
        logging.info("File Full Path and Name : "+ FullFilePath) 
        logging.info("File Content : \n"+ final_file_content)

      else:
       print("Result : File Type Not Supported | File with location :",FullFilePath,"\n")
       other_format_file_list.append(os.path.join(root, filename))
       other_filecount=other_filecount+1
    else:
     unavailable_format_file_list.append(os.path.join(root, filename))
     unavailable_filecount=unavailable_filecount+1
  
"""
  Display Processing Summary
""" 
print("#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#")
print("#         Processing Summary        #")
print("#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#$#")
print('\nTotal File Count : ' + str(filecount))
print('Total File Count Processed : ' + str(parsed_filecount))
print('Total File Count NOT Processed : ' + str(len(content_not_readable_file_list)))
print('All Other File Count Identified : ' + str(other_filecount))
logging.info('Total File Count : ' + str(filecount))
logging.info('Total File Count Processed : ' + str(parsed_filecount))
logging.info('Total File Count NOT Processed : ' + str(len(content_not_readable_file_list)))
logging.info('All Other File Count Identified : ' + str(other_filecount))

pdf_total_file=str(len(pdf_file_list))
print('\nPDF Total File Count Processed : ' + pdf_total_file)
print('File List :')
print(pdf_file_list)
logging.info('PDF Total File Count Processed : ' + pdf_total_file)
logging.info('File List :')
logging.info(pdf_file_list)

doc_total_file=str(len(doc_file_list))
print('\nDOC Total File Count : ' + doc_total_file)
print('File List :')
print(doc_file_list)
logging.info('DOC Total File Count : ' + doc_total_file)
logging.info('File List :')
logging.info(doc_file_list)

other_total_file=str(len(other_format_file_list))
print('\nTotal Other File Count Ignored : ' + other_total_file)
print('File List :')
print(other_format_file_list)
logging.info('Total Other File Count Ignored : ' + other_total_file)
logging.info('File List :')
logging.info(other_format_file_list)

print("\nContent Not Readable File List :")
print(content_not_readable_file_list)
logging.info("Content Not Readble File List :")
logging.info(content_not_readable_file_list)

print('\n File Parsing Program : Completed')  
logging.info('File Parsing Program : Completed')


