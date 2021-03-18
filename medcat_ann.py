from medcat.cat import CAT
from medcat.utils.vocab import Vocab
from medcat.cdb import CDB

import pandas as pd
from medcat.meta_cat import MetaCAT
import numpy as np
import json
from datetime import datetime

import sys


cdb = CDB()
# cdb.load_dict("/home/ubuntu/Tao/KER/medcat_models/0.2.6.2 - snomed_us_ext_names_umls_clean_primary_1M.dat")
cdb.load_dict("/users/k1810895/data/KER/medcat_models/umls_base_wlink_clean_name_400k_mimic.dat")
vocab = Vocab()
vocab.load_dict(path='/users/k1810895/data/KER/medcat_models/base_vocabulary.dat')


mc_negated = MetaCAT(save_dir="/users/k1810895/data/KER/medcat_models/mc_negated/")
mc_negated.load()
mc_skip = MetaCAT(save_dir="/users/k1810895/data/KER/medcat_models/mc_skip/")
mc_skip.load()


cat = CAT(cdb, vocab=vocab, meta_cats=[mc_negated, mc_skip])
cat.train = False
cat.spacy_cat.MIN_ACC = 0.35
cat.spacy_cat.MIN_CONCEPT_LENGTH = 2


# batch = 1000
# for i in range(batch):

# i = int(sys.argv[1])
# print(i)
notes = pd.read_csv("data/mimic_notes.csv")


batch_len = 200
for bt in range(batch_len):
    print(datetime.now(), '  Batch ', bt, ' to process')
    # sql_query = 'select * from noteevents_new_date where row_id%' + str(10000) + '=' + str(i)
    df = notes[notes['row_id']%batch_len == bt]

    batch_size = 1000 # Use 10k if you have a lot of documents
    batch = []
    cnt = 0
    docs = {}

    for name, text in df[['row_id', 'text']].values:
        text = str(text)
        name = str(name)
        cnt += 1
        if len(text) > 0 and name not in docs:
            # This will screwup positions of entites if doc>100k
            npart = int(np.ceil(len(text) / 100000))
            for i in range(npart):
                batch.append((name, text[i*100000:(i+1)*100000]))


        if len(batch) >= batch_size or (cnt == len(df) and len(batch) > 0):
            res = cat.multi_processing(batch, nproc=15, batch_size=batch_size // 100) # Use the correct number of processors

            for name, doc in res:
                if name not in docs:
                    docs[name] = []

                docs[name].extend(doc['entities'])
    #             print(name)
            batch = []
    json.dump(docs, open('data/medcat/batch' + str(bt) +'.json', 'w'))    