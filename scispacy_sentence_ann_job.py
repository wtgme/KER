import scispacy
import spacy
import pandas as pd
import sys
from datetime import datetime
import json
import os.path
# import nltk.data
# tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

print('Start time ', datetime.now())
#SBATCH --nodes=1

from scispacy.linking import EntityLinker

nlp = spacy.load("en_core_sci_scibert")
nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": True, "name": "umls"})
linker = nlp.get_pipe("scispacy_linker")


bt = int(sys.argv[1])
print(bt)


if not os.path.exists('data/scispacy_sentences/batch' + str(bt) +'.json'):

    df = pd.read_csv("data/mimic_notes.csv")
    print('Batch ', bt, ' to process')
    #         sql_query = 'select * from noteevents_new_date where row_id%' + str(10000) + '=' + str(i)
    df = df[df['row_id']%1000 == bt]
    dlist = {}
    for row_id, subject_id, text in df[['row_id', 'subject_id', 'text']].values:
        print('Document ID: ', row_id, datetime.now())
        try:
            doc = nlp(text)
        except Exception as e:
            if 'RuntimeError' in str(e):
                print(str(e))
                continue
        annots = {}
        for ent in doc.ents:
            sent_text = ent.sent.text
            sent_start = ent.sent.start_char
            sent_end = ent.sent.end_char
            sent_text_annots = annots.get(str(sent_start) + '_' + str(sent_end), {})
            sent_annots = sent_text_annots.get('sent_annots', [])
            
            for umls_ent in ent._.kb_ents[:1]:
                annot = {}
                annot['start_char'] = ent.start_char - sent_start
                annot['end_char'] = ent.end_char - sent_start
                annot['text'] = ent.text
                annot['cui'] = umls_ent[0]
                entity = (linker.kb.cui_to_entity[umls_ent[0]])
                annot['name'] = entity.canonical_name
                annot['types'] = ','.join(entity.types)
                annot['ann_id'] = str(row_id) + '_' + str(ent.start_char)
                sent_annots.append(annot)
                annots[str(sent_start) + '_' + str(sent_end)] = {'sent_text': sent_text, 'sent_annots': sent_annots}
        dlist[row_id] = annots


    # data.to_csv('data/scispacy/batch' + str(i) +'.csv', index=None)
    json.dump(dlist, open('data/scispacy_sentences/batch' + str(bt) +'.json', 'w'))   
    
print('End time ', datetime.now())
