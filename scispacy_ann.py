import scispacy
import spacy
import pandas as pd
import sys

from scispacy.linking import EntityLinker

nlp = spacy.load("en_core_sci_scibert")
nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": True, "name": "umls"})

# from sqlalchemy import create_engine
# sql_engine = create_engine('postgresql://ckg:Admin123@10.200.106.114:5432/mimic')

# from elasticsearch import Elasticsearch
# from elasticsearch_dsl import Search, Q
# from elastic_util import * 


# batch = 10000
# for i in range(batch):
#     if i < 946:
#         print('Batch ', i, ' pass')
#         continue
#     else:
#         print('Batch ', i, ' to process')
#         sql_query = 'select * from noteevents_new_date where row_id%' + str(10000) + '=' + str(i)
#         df = pd.read_sql_query(sql_query, con=sql_engine)
#         dlist = []
#         for row_id, subject_id, text in df[['row_id', 'subject_id', 'text']].values:
# #             print('Document ID: ', row_id)
#             try:
#                 doc = nlp(text)
#             except Exception as e:
#                 if 'RuntimeError' in str(e):
#                     continue
#             for ent in doc.ents:
#                 for umls_ent in ent._.kb_ents[:1]:
#                     dlist.append([row_id, subject_id, ent.start_char, ent.end_char, ent.text, umls_ent[0], umls_ent[1]])
#         data = pd.DataFrame(dlist, columns=['row_id', 'subject_id', 'start_char', 'end_char', 'text', 'cui', 'score'])    
#         data['ann_id'] = data['row_id'].astype(str) + data["start_char"].astype(str)


#         es_user = 'elastic'
#         es_pass = 'Admin2019' 
#         es_ip = '10.200.112.204'
#         es_port = '9200'

#         client = Elasticsearch(['http://%s:%s@%s:%s' % (es_user, es_pass, es_ip, es_port)], 
#                                request_timeout=6000, retry_on_timeout=True)

#         doc_type = '_doc'
#         doc_index = 'ann_id'
#         index='mimic_scispacy'

#         result = bulk_insert(client, data, index, doc_type, doc_index)
#         if result == False:
#             print('Current batch:  ', i)
#             break


notes = pd.read_csv("data/mimic_notes.csv")
batch = 1000
for i in range(batch):
# i = int(sys.argv[1])
# print(i)
    if i < 109:
        print('Batch ', i, ' pass')
        continue
    else:
        print('Batch ', i, ' to process')
    #         sql_query = 'select * from noteevents_new_date where row_id%' + str(10000) + '=' + str(i)
        df = notes[notes['row_id']%1000 == i]
        dlist = []
        for row_id, subject_id, text in df[['row_id', 'subject_id', 'text']].values:
    #             print('Document ID: ', row_id)
            try:
                doc = nlp(text)
            except Exception as e:
                if 'RuntimeError' in str(e):
                    continue
            for ent in doc.ents:
                for umls_ent in ent._.kb_ents[:1]:
                    dlist.append([row_id, subject_id, ent.start_char, ent.end_char, ent.text, umls_ent[0], umls_ent[1]])
        data = pd.DataFrame(dlist, columns=['row_id', 'subject_id', 'start_char', 'end_char', 'text', 'cui', 'score'])    
        data['ann_id'] = data['row_id'].astype(str) + data["start_char"].astype(str)


        data.to_csv('data/scispacy/batch' + str(i) +'.csv', index=None)