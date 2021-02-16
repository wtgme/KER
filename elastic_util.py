import json
from datetime import date, datetime
import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))
    
def rec_to_actions(df, INDEX, TYPE, doc_index=None):
    for record in df.to_dict(orient="records"):
        if doc_index:
            yield ('{ "index" : { "_index" : "%s", "_type" : "%s",  "_id" : "%s"}}'% (INDEX, TYPE, record[doc_index]))
        else:
            yield ('{ "index" : { "_index" : "%s", "_type" : "%s"}}'% (INDEX, TYPE))
        yield (json.dumps(record, default=json_serial))
            
def bulk_insert(client, df, INDEX, TYPE, doc_index=None):
    if not client.indices.exists(INDEX):
    #     raise RuntimeError('index %s does not exists'%INDEX)
        client.indices.create(INDEX)
    df = df.where(pd.notnull(df), None)
    n = int(df.shape[0]/10) + 1
    for d in np.array_split(df, n):
        r = client.bulk(rec_to_actions(d, INDEX, TYPE, doc_index)) # return a dict
        if(r["errors"]): 
            print(r)
            return False
    return True

        
def update_to_actions(df, INDEX, TYPE, doc_index=None):
    for record in df.to_dict(orient="records"):
        if doc_index:
            yield ('{ "update" : { "_index" : "%s", "_type" : "%s",  "_id" : "%s"}}'% (INDEX, TYPE, record[doc_index]))
        else:
            yield ('{ "update" : { "_index" : "%s", "_type" : "%s"}}'% (INDEX, TYPE))
        yield ('{ "doc":' + json.dumps(record, default=json_serial) + '}')
        
def bulk_update(client, df, INDEX, TYPE, doc_index=None):
    if not client.indices.exists(INDEX):
        raise RuntimeError('index %s does not exists'%INDEX)
#         client.indices.create(INDEX)
    df = df.where(pd.notnull(df), None)
    n = int(df.shape[0]/10) + 1
    for d in np.array_split(df, n):
        r = client.bulk(update_to_actions(d, INDEX, TYPE, doc_index)) # return a dict
        if(r["errors"]):
            print(r)
            return False
    return True