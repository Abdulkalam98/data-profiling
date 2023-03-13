from typing import List
import os
import re
import sys
import csv
import json
import uuid
import boto3
import datetime
import argparse
import pandas as pd
import pandas_schema
from decimal import *
from pandas_schema import Column, Schema, ValidationWarning
from pandas_schema.validation import CustomElementValidation,DateFormatValidation
import datetime
from pandas_schema.validation import _SeriesValidation


def s3_download(download_file,s3_bucket,region_name,obj):
    #download config file from s3
    try:
        s3 = boto3.client('s3', region_name=region_name)
        ingest_key=obj+download_file
        s3.download_file(s3_bucket,ingest_key,download_file)
        print(download_file + " File Download from S3")
        return True
    except Exception as e:
        print(download_file + " File Downloading Failed")
        print(str(e))
        return False

def get_config(download_file,s3_bucket,region_name,object):
    #read the config_file
    try:
        flag=False
        flag=s3_download(download_file,s3_bucket,region_name,object)
        if flag:
            #Read the credentials from the config file
            with open(download_file,"r") as config_file:
                jsonprop = json.load(config_file)
            return jsonprop

    except Exception as e:
        print("unable to read config file")
        print(str(e))



class CustomDateFormatValidation(_SeriesValidation):
    """
    Checks that each element in this column is a valid date according to a provided format string
    """

    def __init__(self, date_format: str, **kwargs):
        """
        :param date_format: The date format string to validate the column against. Refer to the date format code
            documentation at https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior for a full
            list of format codes
        """
        self.date_format = date_format
        super().__init__(**kwargs)


    @property
    def default_message(self):
        return 'does not match the date format string "{}"'.format(self.date_format)

    def valid_date(self, val):
        try:
            if(str(val)=="nan"):
                return True
            else:
                #print(val)
                #print(self.date_format)
                #print(datetime.strptime(val, self.date_format))
                datetime.datetime.strptime(val, self.date_format)

                return True
        except:
            return False

    def validate(self, series: pd.Series) -> pd.Series:
        return series.astype(str).apply(self.valid_date)


'''
Below functions are used to check the value of each column correponds to their data type as mentioned in schema.csv
we will validate each elements of dataset, whether its value is coming as per the required format or not.
'''

def check_decimal(dec):
    try:
        Decimal(dec)
    except InvalidOperation:
        return False
    return True


def check_int(num):
    try:
        if(str(num)=="nan"):
                ""
        else:
            int(num)
    except ValueError:
        return False
    return True

def check_string(string):
    try:
        str(string)
    except ValueError:
        return False
    return True

def check_date(string):
    try:
        datetime.datetime.strptime(string,"%Y-%m-%d")
    except ValueError:
        return False
    return True


def do_validation(fileName,encoding,schemaName,fileHeader,delimiter,chunkSize):

    # read the data
    print("Reading ", schemaName)
    schema1=pd.read_csv(schemaName,header=None)
    print("encoding in do_validation : " + encoding)
    print("the schema1 value is : " + str(schema1))
    row_count_schema = len(schema1.index) # count the row (= columns names) in the _schema.csv file
    print("Count of columns from the schema file: ", row_count_schema)

    #data = pd.read_csv(fileName)


    # define validation elements
    decimal_validation = [CustomElementValidation(lambda d: check_decimal(d), 'is not decimal')]
    int_validation = [CustomElementValidation(lambda i: check_int(i), 'is not integer')]
    string_validation = [CustomElementValidation(lambda i: check_string(i), 'is not string')]
    null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]


    #Creation of schema from file
    names = []
    schemaDtype = {}
    for i in schema1.index:
        schemaDtype[schema1.iloc[i][0]] = "object"
        if(schema1.iloc[i][1]=="int"):
            names.append(Column(schema1.iloc[i][0],int_validation))
        elif(str(schema1.iloc[i][1])=="nan"):
            names.append(Column(schema1.iloc[i][0],string_validation))
        elif(schema1.iloc[i][1]=="datetime"):
            dateTimeFormat = schema1.iloc[i][2]
            names.append(Column(schema1.iloc[i][0],[CustomDateFormatValidation(dateTimeFormat)]))
        elif(schema1.iloc[i][1]=="date"):
            dateFormat = schema1.iloc[i][2]
            names.append(Column(schema1.iloc[i][0],[CustomDateFormatValidation(dateFormat)]))
        elif(schema1.iloc[i][1]=="float"):
            names.append(Column(schema1.iloc[i][0],decimal_validation))

    print("The value of names is : " + str(names))
    # define validation schema
    schema=pandas_schema.Schema(names)
    print("The value of schema is : " + str(schema))
    fileHead = fileHeader

    filename_obj = pd.read_csv(fileName,delimiter=delimiter,nrows=100,encoding=encoding)
    row_count_filename = len(filename_obj.columns) # count of columns in current file
    print("Number of columns in the file:" , row_count_filename )

    if row_count_schema != row_count_filename:
        print("Column counts mismatch")
        return -1

    if filename_obj.shape[0] > 1 :
        # apply validation
        print("entered if clause")
        print("encoding in if caluse : " + encoding)
        for i,df in enumerate(pd.read_csv(fileName,delimiter=delimiter,dtype=schemaDtype, chunksize=chunkSize,encoding=encoding,names=schema1.iloc[:,0]), 1):
            print("encoding in for loop of if clause : " + encoding)
            if(fileHead=="True"):
                #print(df.iloc[:1])
                print("Dropping first record")
                #df = df.drop([0])
                df = df.iloc[1:]
                #print("Error checkpoint")
            errors = schema.validate(df)
            print("errors is : " + str(errors))
            errors_index_rows = [e.row for e in errors]
            print("errors_index_rows is : " + str(errors_index_rows))
            data_clean = df.drop(index=errors_index_rows)
            #data_error = data.drop(index=list(set(range(0,data.shape[0]))-set(errors_index_rows)))


            # save clean and error data in respective location
            if(fileHead=="True"):
                if(errors):
                    pd.DataFrame({'err_disc':errors}).to_csv('errors_logs_'+fileName,index=False,mode='a')
                data_clean.to_csv(fileName+"_tmp",index=False,mode='a',sep=delimiter)
                fileHead = "False"
            else:
                if(errors):
                    pd.DataFrame({'err_disc':errors}).to_csv('errors_logs_'+fileName,index=False,mode='a',header=False)
                data_clean.to_csv(fileName+"_tmp",index=False,mode='a',header=False,sep=delimiter)
        os.rename(fileName+"_tmp",'processed_'+fileName)
        tmp=str('processed_'+fileName).replace('preprocessed_','')
        os.rename('processed_'+fileName,tmp)
        return 0

def write_error_delim(line,file):
    file=open('error_delimiter_'+file, 'a')
    #print("Writing Error File")
    file.write(line.strip()+'\n')
    file.close()

def write_processed_file(line,file):
    file=open('preprocessed_'+file, 'a')
    #print("Writing preProcessedFile")
    file.write(line.strip()+'\n')
    file.close()

def check_delimiter(inputFile,schemaFile):
    file=open(inputFile,'r')
    print("Validating Delimiter Check")
    df_schema=pd.read_csv(schemaFile,header=None)
    schema_col_len=len(df_schema)
    schema_col=list(df_schema.iloc[:,0])
    cols=','.join(schema_col)
    write_error_delim(cols,inputFile)
    while True:
        line=file.readline()
        if not line:
            break
        if line.count(',')==schema_col_len-1:
            #print('Matching the delimiters with Schema Column Length')
            write_processed_file(line,inputFile)
        else:
            #print('Not Matching the delimiters with Schema Column Length')
            write_error_delim(line,inputFile)

def check_cnt_metric(file_name,header):
    try:
        lines=[]
        with open(file_name,'r+') as file:
            lines=file.readlines()
        cnt_met=lines[-1]
        if header:
            total_records=len(lines)-2
        else:
            total_records=len(lines)-1
        if total_records==cnt_met:
            print("File Count is Matching with Control File Validation")
        else:
            print("File Count is Not Matching with Control File Validation")
    except Exception as e:
        print(e)
        return(str(e))

def check_not_null(df,not_null_columns,header,file):
    file=file.replace('preprocessed_','')
    #print("Profiling NOT NULL Validation")
    with open("not_null_"+file, "a+") as f:
        df_not_null=df[df[not_null_columns].isnull().any(1)]
        notNullList=list(df_not_null.index)
        df_not_null.to_csv(f,header=header,index=False,mode='a',line_terminator="\n")

    #print(df_not_null.index)
    header=False
    return(notNullList,header)

def check_grain_col(df,grain_columns,header,file):
    file=file.replace('preprocessed_','')
    #print("Profiling Grain Column Validation")
    with open("grain_"+file,'a+') as f:
        df_grain=df.groupby(grain_columns).filter(lambda x: len(x) > 1)
        #print(df_grain)
        df_grain.to_csv(f,header=header,index=False,mode='a',line_terminator="\n")
        grainList=list(df_grain.index)
    header=False
    return(grainList,header)

def check_col_displace(filename,schemaFile,delimiter,encoding):
    df2 = pd.read_csv(filename,delimiter=delimiter,nrows=100,encoding=encoding)
    file_column = list(df2.columns)
    df3 = pd.read_csv(schemaFile, header =None)
    schema_column = df3[0].tolist()
    file_column_string = ''.join(file_column)
    schema_column_string= ''.join(schema_column)
    if file_column_string.casefold() == schema_column_string.casefold():
        print("Columns are in place")
    else:
        print("Columns are displaced in source file")

def referential_key_check(masterFile,masterColumns,childFile,childColumns,delimiter):
    columns=[]
    row={}
    master = set()
    child = set()
    masterListCol=masterColumns.split(',')
    childListCol=childColumns.split(',')
    processedChildFile='processed_child_'+childFile

    # Read the data from the Master file
    with open(masterFile, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            #print(row)
            master.add('-'.join([row[i].strip() if row[i] is not None  else 'NULL' for i in masterListCol]))

    # Read the data from the Child file
    with open(childFile, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            child.add('-'.join([row[i].strip() if row[i] is not None  else 'NULL' for i in childListCol]))

    # Check the foreign key constraint
    reference_key_violations = child - master

    # Print the result
    print('Reference Key Violation:')
    print(reference_key_violations)

    #Read the Child File and generate the processed Child File
    with open(childFile, 'r') as input_file,open(processedChildFile,'w',newline='') as output_file:
        reader = csv.DictReader(input_file)
        print("Processing the child File " + childFile)
        for i,row in enumerate(reader):
            temp='-'.join([row[x].strip() if row[x] is not None  else 'NULL' for x in masterListCol])
            columns=row.keys()
            if temp not in reference_key_violations:
                print("Valid_Records: "+temp + ' Index: '+str(i))
                writer = csv.DictWriter(output_file, delimiter=delimiter,
                            fieldnames=row)
                writer.writerow(row)
    #Add Columns to a processed child File
    col=delimiter.join(x for x in columns)
    with open(processedChildFile, "r+") as file: data = file.read(); file.seek(0); file.write(col + '\n'+ data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Getting Config file location')
    parser.add_argument('-f', help="Provide the local config filename")
    parser.add_argument('-c', help="Provide the config filename in s3", default=False)
    parser.add_argument('-s3', help="Provide the input bucketname", default=False)
    parser.add_argument('-r',help="Provie the region name",default=False)
    parser.add_argument('-p', help="Provide the input path",default=False)


    args = parser.parse_args()

    localConfigFile=args.f
    configFile=args.c
    s3Bukcet=args.s3
    s3Path=args.p
    region=args.r

    # work_dir = "/tmp/data_profiling/"
    # if not os.path.exists(work_dir):
    #     os.makedirs(work_dir)
    if getattr(sys, 'frozen', False):
        app_path = os.path.dirname(sys.executable)
    else:
        app_path = os.path.dirname(os.path.abspath(__file__))
    dname = os.path.dirname(app_path)
    #os.chdir(app_path)
    # app_path=sys.path[0]
    #print(app_path)
    os.chdir(app_path)
    print('Current Working Directory: '+app_path)
    jsonProp={}
    if configFile:
        #Download the config file from s3 and read the file
        jsonProp=get_config(configFile,s3Bukcet,region,s3Path)
        inputFileName=jsonProp['inputFileName']
        schemaFileName=jsonProp['schemaFileName']
        inputBucket=jsonProp['inputBucket']
        inputDir=jsonProp['inputDir']
        schemaBucket=jsonProp['schemaBucket']
        schemaDir=jsonProp['schemaDir']
        #Downloading the file from S3
        s3_download(inputFileName,inputBucket,region,inputDir)
        s3_download(schemaFileName,schemaBucket,region,schemaDir)
    else:
        with open(localConfigFile,"r") as config_file:
            jsonProp = json.load(config_file)

    inputFileName=jsonProp['inputFileName']
    encoding=jsonProp['encoding']
    schemaFileName=jsonProp['schemaFileName']
    fileHead=jsonProp['fileHead']
    fileDelim=jsonProp['fileDelim']
    notNullColumns=jsonProp['notNullColumns']
    grainColumns=jsonProp['grainColumns']
    chunkSize=jsonProp['chunkSize']
    #specialCharacters=jsonProp['special_characters']
    cntlFileValidation=jsonProp['cntlValidation']
    referenceKeyCheck=jsonProp['referenceKeyCheck']
    referentialKeyFile=jsonProp['referentialKeyFile']


    if cntlFileValidation:
        check_cnt_metric(inputFileName,fileHead)

    check_delimiter(inputFileName,schemaFileName)

    preProcessedFile='preprocessed_'+inputFileName
    notNullColList=notNullColumns.split(',')
    grainColList=grainColumns.split(',')

    notNullIndex=[]
    grainIndex=[]
    grainNotNullIndex=[]
    header=fileHead
    print("Profiling NOT NULL Validation")
    for chunk in pd.read_csv(preProcessedFile, chunksize=chunkSize):
        notNullList,header=check_not_null(chunk,notNullColList,header,preProcessedFile)
        notNullIndex.extend(notNullList)
        #print(notNullIndex)
    header=fileHead
    print("Profiling Grain Column Validation")
    for chunk in pd.read_csv(preProcessedFile, chunksize=chunkSize):
        grainList,header=check_grain_col(chunk,grainColList,header,preProcessedFile)
        grainIndex.extend(grainList)
        #print(grainIndex)
    grainNotNullIndex.extend(grainIndex)
    grainNotNullIndex.extend(notNullIndex)
    #print(grainNotNullIndex)
    if fileHead:
        grainNotNullIndex=list(map(lambda x:x+1,grainNotNullIndex))

    print("Preprocessing the File")
    with open(preProcessedFile, 'r+') as fp:
        # read an store all lines into list
        lines = fp.readlines()
        # move file pointer to the beginning of a file
        fp.seek(0)
        # truncate the file
        fp.truncate()

        # start writing lines
        # iterate line and line number
        print("Removing Non-Ascii Characters from "+ inputFileName)
        for number, line in enumerate(lines):
            line1=line.encode('ascii',errors='ignore').decode()
            # delete line number 5 and 8
            # note: list index start from 0
            if number not in grainNotNullIndex:
                fp.write(line1)
    do_validation(preProcessedFile,encoding,schemaFileName,fileHead,fileDelim,chunkSize)

    check_col_displace(preProcessedFile,schemaFileName,fileDelim,encoding)

    #Referential Key constraint check with master table and the child table
    if referenceKeyCheck:
        for keys in referentialKeyFile:
            if configFile:
                masterFileName=jsonProp['masterFileName']
                masterBucket=jsonProp['masterBucket']
                masterDir=jsonProp['masterDir']
                masterReferencekey=keys['masterReferencekey']
                childFileName=keys['fileName']
                childFileBucket=keys['fileBucket']
                childFilepath=keys['filePath']
                childReferentialKey=keys['childReferenceKey']
                s3_download(masterFileName,masterBucket,region,masterDir)
                s3_download(childFileName,childFileBucket,region,childFilepath)
                referential_key_check(masterFileName,masterReferencekey,childFileName,childReferentialKey,fileDelim)
            else:
                masterFileName=jsonProp['masterFileName']
                masterReferencekey=keys['masterReferencekey']
                childFileName=keys['fileName']
                childReferentialKey=keys['childReferenceKey']
                referential_key_check(masterFileName,masterReferencekey,childFileName,childReferentialKey,fileDelim)
