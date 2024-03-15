from medcat.cat import CAT
from medcat.utils.vocab import Vocab
from medcat.cdb import CDB

import pandas as pd
from medcat.meta_cat import MetaCAT
import numpy as np
import json
from datetime import datetime
import os.path

import sys
# import nltk.data
# tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')


print('Start time ', datetime.now())

bt = int(sys.argv[1])
print(bt)



if not os.path.exists('data/medcat_sentence/batch' + str(bt) +'.json'):
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


    df = pd.read_csv("data/mimic_notes.csv")


    # batch_len = 1000
    # for bt in range(batch_len):

    print(datetime.now(), '  Batch ', bt, ' to process')
    # sql_query = 'select * from noteevents_new_date where row_id%' + str(10000) + '=' + str(i)
    df = df[df['row_id']%1000 == bt]
    dlist = {}

    for row_id, doc in df[['row_id', 'text']].values:
        doc = str(doc)
        #print(datetime.now(), ' document ', did)
        
        doc_spacy = cat(doc)
        annots = {}

        for ent in doc_spacy.ents:
            sent_text = ent.sent.text
            sent_start = ent.sent.start_char
            sent_end = ent.sent.end_char
            sent_text_annots = annots.get(str(sent_start) + '_' + str(sent_end), {})
            sent_annots = sent_text_annots.get('sent_annots', [])

            annot = {}
            annot['start_char'] = ent.start_char - sent_start
            annot['end_char'] = ent.end_char - sent_start
            annot['text'] = ent.text
            annot['cui'] = ent._.cui
            annot['tui'] = ent._.tui
            annot['name'] = cdb.cui2pretty_name[ent._.cui]
            annot['types'] = cdb.tui2name[ent._.tui]
            annot['meta_anns'] = ent._.meta_anns
            annot['ann_id'] = str(row_id) + '_' + str(ent.start_char)
            sent_annots.append(annot)
            annots[str(sent_start) + '_' + str(sent_end)] = {'sent_text': sent_text, 'sent_annots': sent_annots}
        dlist[row_id] = annots
        
    json.dump(dlist, open('data/medcat_sentence/batch' + str(bt) +'.json', 'w'))    

print('End time ', datetime.now())
