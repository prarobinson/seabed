#!/usr/bin/python

import sys
from suds import null, WebFault
from suds.client import Client

def get_records(proxy_obj, name, offset_number):
    recs = proxy_obj.service.getAphiaRecords(name, like=True, fuzzy=True, marine_only=False, offset=offset_number)
    return recs



def get_records_by_vernacular(proxy_obj, vern_name, offset_number):
    recs = proxy_obj.service.getAphiaRecordsByVernacular(vern_name, like=True, marine_only=False, offset=offset_number)
    return recs



def get_all_worms_records(proxy_obj, taxon_name, mode='scientific'):  # This function originated from the WoRMS SOAP service example code.
    start = 0
    max_capacity = 50
    records = []
    print('-- mode is %s:' % mode)
    print('-- get_all_worms_records: fetching records', start, 'to', max_capacity, 'for taxon', taxon_name)
    if mode is 'scientific':
        a = get_records(proxy_obj, str(taxon_name), start)
    elif mode is 'vernacular':
        a = get_records_by_vernacular(proxy_obj, str(taxon_name), start)
    if a is not None:
        for i in a:
            records.append(i)
        while len(records) == max_capacity:
            start += max_capacity
            max_capacity += max_capacity
            print('-- get_all_worms_records: fetching records', start, 'to', max_capacity, 'for taxon', taxon_name)
            b = get_records(proxy_obj, str(taxon_name), start)
            if b is not None:
                for i in b:
                    records.append(i)
        print('-- get_all_worms_records: returning', len(records), 'records for taxon', taxon_name)
        return records
    else:
        return None

if __name__ == '__main__':

    wsdlObjectWoRMS = Client('http://www.marinespecies.org/aphia.php?p=soap&wsdl=1')

    # Replace these with your desired scientific names.
    target_name = sys.argv[1]

    # Uncomment this to instead search based on vernacular name
    #mode = 'vernacular'
    
    ret = get_all_worms_records(wsdlObjectWoRMS, target_name)   
    print('\n')

    
