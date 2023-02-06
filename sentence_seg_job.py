import scispacy
import spacy
import pandas as pd
import sys
from datetime import datetime
import json
import os.path
# from scispacy.linking import EntityLinker


print('Start time ', datetime.now())
#SBATCH --nodes=1
i = int(sys.argv[1])
print(i)
    
if not os.path.exists('data/scispacy_sentences/batch' + str(i) +'.json'):


    nlp = spacy.load("en_core_sci_scibert")
#     nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": True, "name": "umls"})
#     linker = nlp.get_pipe("scispacy_linker")

    df = pd.read_csv("data/mimic_notes.csv")
    # if i < 0:
    #     print('Batch ', i, ' pass')
    # else:
    print('Batch ', i, ' to process')
    #         sql_query = 'select * from noteevents_new_date where row_id%' + str(10000) + '=' + str(i)
    df = df[df['row_id']%1000 == i]
    dlist = {}
    for row_id, subject_id, text in df[['row_id', 'subject_id', 'text']].values:
    #             print('Document ID: ', row_id)
        try:
            doc = nlp(text)
        except Exception as e:
            if 'RuntimeError' in str(e):
                print('Seg sentences: '+str(e))
                continue
        sentences = []
        for sent in doc.sents:
            seg = {}
            seg['sen_text'] = sent.text
            seg['sen_start_char'] = sent.start_char
            seg['sen_end_char'] = sent.end_char
            seg['sen_start'] = sent.start
            seg['sen_end'] = sent.end            
#             annots = []
#             try:
#                 sen = nlp(sent.text)
#             except Exception as e:
#             if 'RuntimeError' in str(e):
#                 print('Annotate sentences: '+str(e))
#                 continue
#             for ent in sen.ents:
#                 for umls_ent in ent._.kb_ents[:1]:
#                     annot = {}
#                     annot['ann_start_char'] = ent.start_char
#                     annot['ann_end_char'] = ent.end_char
#                     annot['ann_text'] = ent.text
#                     annot['cui'] = umls_ent[0]
#                     entity = (linker.kb.cui_to_entity[umls_ent[0]])
#                     annot['name'] = entity.canonical_name
#                     annot['types'] = ','.join(entity.types)
#                     annots.append(annot)
#             seg['annotation'] = annots
            sentences.append(seg)        
        dlist[row_id] = sentences

    json.dump(dlist, open('data/scispacy_sentences/batch' + str(i) +'.json', 'w'))  

    print('End time ', datetime.now())