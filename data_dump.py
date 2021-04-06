from elasticsearch import Elasticsearch, helpers
import sys, json, os

es = Elasticsearch('http://elastic:Admin2019@10.200.112.204:9200', request_timeout=6000, retry_on_timeout=True)

# with open('done.log') as f:
#     content = f.readlines()
# # you may also want to remove whitespace characters like `\n` at the end of each line
# content = [x.strip() for x in content] 
# content = []


if not es.ping():
    raise ValueError("Connection failed")
    
def load_json(directory):
    " Use a generator, no need to load all in memory"
    for filename in os.listdir(directory):
        if (filename.endswith('.json')):
            print(filename)
            with open(directory + filename, 'r') as open_file:
                fs = json.load(open_file)
                for did in fs.keys():
                    for an in fs[did]:
                        an['row_id'] = int(did)
                        yield an
            
            
# path = 'data/scispacy/'
# es.indices.delete(index='mimic_scispacy')

# helpers.bulk(es, load_json(path), index='mimic_scispacy', doc_type='_doc')

# print('Scispacy annotations done')




path = 'data/medcat/'
es.indices.delete(index='mimic_medcat')
helpers.bulk(es, load_json(path), index='mimic_medcat', doc_type='_doc')

print('Medcat annotations done')