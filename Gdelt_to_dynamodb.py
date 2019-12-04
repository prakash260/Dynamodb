#!/usr/bin/python3.7
import csv
import sys
import os
import boto3
from botocore.exceptions import ClientError,ParamValidationError
#from multiprocessing import Process
import concurrent.futures
import multiprocessing
from datetime import datetime, timedelta
#from multiprocessing.pool import ThreadPool
 
column_names = ['globaleventid', 'day', 'monthyear', 'year', 'fractiondate', 'actor1code', 'actor1name',
                'actor1countrycode', 'actor1knowngroupcode', 'actor1ethniccode', 'actor1religion1code',
                'actor1religion2code', 'actor1type1code', 'actor1type2code', 'actor1type3code', 'actor2code',
                'actor2name', 'actor2countrycode', 'actor2knowngroupcode', 'actor2ethniccode', 'actor2religion1code',
                'actor2religion2code', 'actor2type1code', 'actor2type2code', 'actor2type3code', 'isrootevent',
                'eventcode', 'eventbasecode', 'eventrootcode', 'quadclass', 'goldsteinscale', 'nummentions',
                'numsources', 'numarticles', 'avgtone', 'actor1geo_type', 'actor1geo_fullname', 'actor1geo_countrycode',
                'actor1geo_adm1code', 'actor1geo_lat', 'actor1geo_long', 'actor1geo_featureid', 'actor2geo_type',
                'actor2geo_fullname', 'actor2geo_countrycode', 'actor2geo_adm1code', 'actor2geo_lat', 'actor2geo_long',
                'actor2geo_featureid', 'actiongeo_type', 'actiongeo_fullname', 'actiongeo_countrycode',
                'actiongeo_adm1code', 'actiongeo_lat', 'actiongeo_long', 'actiongeo_featureid', 'dateadded',
                'sourceurl']
 
column_types = [int, int, int, int, float, str, str, str, str, str, str, str, str, str, str, str, str, str, str, str,
                str, str, str, str, str, bool, str, str, str, int, float, int, int, int, float, int, str, str, str,
                float, float, int, int, str, str, str, float, float, int, int, str, str, str, float, float, int, int,
                str]
 
 
 
table_name = "deltattl"
items = []
client = boto3.client('dynamodb', region_name="us-east-1")
 
 
def do_batch_write(items, client, table_name):
    try:
        response = client.batch_write_item(RequestItems={table_name: items})
        print('status code %s: RequestId: %s' %(response['ResponseMetadata']['HTTPStatusCode'],
                                                response['ResponseMetadata']['RequestId']))
    except ParamValidationError:
        #print(items)
        response = client.batch_write_item(RequestItems={table_name: items})
        print('status code %s: RequestId: %s' % (response['ResponseMetadata']['HTTPStatusCode'],
                                                 response['ResponseMetadata']['RequestId']))
 
    while len(response['UnprocessedItems']) != 0:
        try:
            response = client.batch_write_item(RequestItems={table_name: items})
        except ParamValidationError:
            response = client.batch_write_item(RequestItems={table_name: items})
            print('status code %s: RequestId: %s' %(response['ResponseMetadata']['HTTPStatusCode'],
                                                    response['ResponseMetadata']['RequestId']))
 
 
def process(file):
    batch_of_item = []
    row = {}
    num = 0
 
 
    with open(file) as games:
        print('openfile print', file)
        game_reader = csv.reader(games, delimiter='\t')
        bad_item = '{}.bad_item'.format(file)
        for lst in game_reader:
            with open(bad_item, "a+") as bad:
                    for colunm_number, colunm_name in enumerate(column_names):
                        try:
                            if (lst[colunm_number]) != '':
                            # print(colunm_name,lst[colunm_number])
                                if (column_types[colunm_number] == int) or (column_types[colunm_number] == float):
                                    row[colunm_name] = {'N': column_types[colunm_number](lst[colunm_number])}
                                    row[colunm_name] = {'N': (lst[colunm_number])}
                                elif (column_types[colunm_number] == bool):
                                    row[colunm_name] = {'BOOL': column_types[colunm_number](lst[colunm_number])}
                                else:
                                    row[colunm_name] = {'S': column_types[colunm_number](lst[colunm_number])}
 
                        except ValueError as e:
                            bad.write(column_names[colunm_number]+ "\n")
                            bad.write(str(lst) + "\n")
                            row = {}
                            break
 
            if len(row) != 0:
                tt1 = (datetime.now() + timedelta(hours = 1)).timestamp()
                ttl = str(int(tt1))
                row['ttl'] = {'N': ttl}
                print(row)
                batch_of_item.append({'PutRequest': {'Item': row.copy()}})
                num += 1
            if num == 25:
                do_batch_write(batch_of_item, client, table_name)
                batch_of_item = []
                num = 0
        if len(batch_of_item) != 0:
           do_batch_write(batch_of_item, client, table_name)
           bad.close()
 
 
 
def main():
    with concurrent.futures.ProcessPoolExecutor(1) as executor:
         root = "/data1/milestone1"
         for (dirpath, dirnames, filenames) in os.walk(root):
             print(filenames)
             executor.map(process, filenames)
 
 
#def main():
    #files = []
#    root = "/data/milestone"
#    for (dirpath, dirnames, filenames) in os.walk(root):
#        for f in filenames:
#            process(f)
 
 
 
if __name__=="__main__":
    main()
