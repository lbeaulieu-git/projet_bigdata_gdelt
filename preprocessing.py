# -*- coding: utf-8 -*-


#######################################
###### Prepocessing GDELT Project #####                    
#######################################

#######################################
######### Import libraries ############                    
#######################################

import validators
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Pool
import pandas as pd
import pandas as pd
import numpy as np
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import datetime
import time
import sys
#import csv

#csv.field_size_limit(sys.maxsize)

#######################################
############# SCRAPPING ###############                    
#######################################

# URL : masterfilelist.txt

def masterfilelist(nb_url, start_date, end_date):

    response = requests.get("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt")
    content = response.content.decode("utf-8") 
    l = content.split('\n')[-nb_url:]

    liste = list()
    for i in l: liste.append(i.split(" ")[-1])

    df = pd.DataFrame(liste, columns=['url'])
    df['date_str'] = df['url'].apply(lambda x : x.split("/")[-1].split(".")[0][0:12])
    df = df.iloc[:df.shape[0]-1,:]
    df["date"] = pd.to_datetime(df["date_str"], format='%Y%m%d%H%M')

    start_datem = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    end_datem = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
    df = df.loc[(df['date'] >= start_datem) & (df['date'] <= end_datem)]
    
    df['type_csv'] = df['url'].apply(lambda x : x.lower().split(".csv")[0].split(".")[-1])

    df['id'] = df['date_str']+'_'+df['type_csv']
    
    df = df.drop(columns=['date'])
    
    return df

# URL : masterfilelist-translation.txt

def masterfilelist_translation(nb_url, start_date, end_date):

    response = requests.get("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt")
    content = response.content.decode("utf-8") 
    l = content.split('\n')[-nb_url:]

    liste = list()
    for i in l: liste.append(i.split(" ")[-1])

    df = pd.DataFrame(liste, columns=['url_translation'])
    df['date_str_translation'] = df['url_translation'].apply(lambda x : x.split("/")[-1].split(".")[0][0:12])
    df = df.iloc[:df.shape[0]-1,:]
    df["date"] = pd.to_datetime(df["date_str_translation"], format='%Y%m%d%H%M')

    start_datem = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    end_datem = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
    df = df.loc[(df['date'] >= start_datem) & (df['date'] <= end_datem)]
    
    df['type_csv_translation'] = df['url_translation'].apply(lambda x : '_'.join(x.lower().split(".csv")[0].split(".")[-2:]))

    df['type_csv'] = df['url_translation'].apply(lambda x : x.lower().split(".csv")[0].split(".")[-1])

    df['id'] = df['date_str_translation']+'_'+df['type_csv']

    df = df.drop(columns=['type_csv', 'date'])
    
    return df

###################################################
############# VERIFIFYI URL #######################                    
###################################################

def verify_url(u):
    if validators.url(u) == True:
        return True
    else : 
        return False
    
# Merge masterfile.txt and masterfile_translation.txt

def merge_table(df, df_translation):
    
    # - left join des tableaux
    # - Première séléction : Supprimons les lignes où des NaN apparait
    # - Vérification URL
    
    result = df.merge(df_translation, on='id', how='left').dropna(axis='rows')
    result['work'] = result['url'].apply(lambda x : verify_url(x))
    result['work_translation'] = result['url_translation'].apply(lambda x : verify_url(x))
    return result

###################################################
############# CLEAN DATASET #######################                    
###################################################

def clean_dataset(df):
    
    dk = df.groupby('date_str').count()[['id']]
    liste = dk[dk.id < 3].index.tolist()
    for item in liste:
        df = df.loc[df['date_str']!=item]

    liste = df.loc[df['work']==False]['date_str'].unique().tolist()
    for item in liste:
        df = df.loc[df['date_str']!=item]

    liste = df.loc[df['work_translation']==False]['date_str'].unique().tolist()
    for item in liste:
        df = df.loc[df['date_str']!=item]
            
    return df


###################################################
############# FUSION TABLE ########################                    
###################################################

def concat_table(result):
    
    # Séparation des données de base et de translation ET concaténation 
    
    df_base = result[['url', 'type_csv']]

    df_translation = result[['url_translation', 'type_csv_translation']]

    df_translation = df_translation.rename(columns={'url_translation': "url", 'type_csv_translation': "type_csv"})

    final = pd.concat([df_base, df_translation])
    
    return final

###################################################
############# LECTURE ZIPS ########################                    
###################################################

def read_zip(final):
    
    export               = final.loc[final['type_csv'] == 'export', 'url']
    mentions             = final.loc[final['type_csv'] == 'mentions', 'url']
    gkg                  = final.loc[final['type_csv'] == 'gkg', 'url']
    translation_export   = final.loc[final['type_csv'] == 'translation_export', 'url']
    translation_mentions = final.loc[final['type_csv'] == 'translation_mentions', 'url']
    translation_gkg      = final.loc[final['type_csv'] == 'translation_gkg', 'url']
    
    df_export               = list()
    df_mentions             = list()
    df_gkg                  = list()
    df_translation_export   = list()
    df_translation_mentions = list()
    df_translation_gkg      = list()
    
    for i in export.tolist():
        url = urlopen(i) 
        k = i.split("/")[-1].split(".zip")[0]
        zipfile = ZipFile(BytesIO(url.read()))
        FFdata = pd.read_csv(zipfile.open(k), header=None, sep="\t", engine='python', encoding = 'latin-1', error_bad_lines=False)
        df_export.append(FFdata)
        
    for i in mentions.tolist():
        url = urlopen(i) 
        k = i.split("/")[-1].split(".zip")[0]
        zipfile = ZipFile(BytesIO(url.read()))
        FFdata = pd.read_csv(zipfile.open(k), header=None, sep="\t", engine='python', encoding = 'latin-1', error_bad_lines=False)
        df_mentions.append(FFdata)
        
    for i in gkg.tolist():
        url = urlopen(i) 
        k = i.split("/")[-1].split(".zip")[0]
        zipfile = ZipFile(BytesIO(url.read()))
        FFdata = pd.read_csv(zipfile.open(k), header=None, sep="\t", engine='python', encoding = 'latin-1', error_bad_lines=False)
        df_gkg.append(FFdata)
        
    for i in translation_export.tolist():
        url = urlopen(i) 
        k = i.split("/")[-1].split(".zip")[0]
        zipfile = ZipFile(BytesIO(url.read()))
        FFdata = pd.read_csv(zipfile.open(k), header=None, sep="\t", engine='python', encoding = 'latin-1', error_bad_lines=False)
        df_translation_export.append(FFdata)
        
    for i in translation_mentions.tolist():
        url = urlopen(i) 
        k = i.split("/")[-1].split(".zip")[0]
        zipfile = ZipFile(BytesIO(url.read()))
        FFdata = pd.read_csv(zipfile.open(k), header=None, sep="\t", engine='python', encoding = 'latin-1', error_bad_lines=False)
        df_translation_mentions.append(FFdata)
        
    for i in translation_gkg.tolist():
        url = urlopen(i) 
        k = i.split("/")[-1].split(".zip")[0]
        zipfile = ZipFile(BytesIO(url.read()))
        FFdata = pd.read_csv(zipfile.open(k), header=None, sep="\t", engine='python', encoding = 'latin-1', error_bad_lines=False)
        df_translation_gkg.append(FFdata)
        
    export = pd.concat(df_export)
    mentions = pd.concat(df_mentions)
    gkg = pd.concat(df_gkg)

    export_translation = pd.concat(df_translation_export)
    mentions_translation = pd.concat(df_translation_mentions)
    gkg_translation = pd.concat(df_translation_gkg)
        
    return export, mentions, gkg, export_translation, mentions_translation, gkg_translation



###################################################
############# PRE PROCESSING BASE TABLE ###########
###################################################

def rename_columns(export, mentions, gkg, translation_export, translation_mentions, translation_gkg):
    
    for i in range(export.shape[1]):
        export.rename({i: 'export_'+str(i)}, axis=1, inplace=True)
        
    for i in range(mentions.shape[1]):
        mentions.rename({i: 'mentions_'+str(i)}, axis=1, inplace=True)    
        
    for i in range(gkg.shape[1]):
        gkg.rename({i: 'gkg_'+str(i)}, axis=1, inplace=True)
        
    for i in range(translation_export.shape[1]):
        translation_export.rename({i: 'export_translation_'+str(i)}, axis=1, inplace=True)
        
    for i in range(translation_mentions.shape[1]):
        translation_mentions.rename({i: 'mentions_translation_'+str(i)}, axis=1, inplace=True)    
        
    for i in range(translation_gkg.shape[1]):
        translation_gkg.rename({i: 'gkg_translation_'+str(i)}, axis=1, inplace=True)            
    
    return export, mentions, gkg, translation_export, translation_mentions, translation_gkg

###################################################
############# DATE TO DATETIME ####################
###################################################

def date_to_datime(export, mentions, gkg, export_translation, mentions_translation, gkg_translation):
    export["export_1"] = pd.to_datetime(export["export_1"], format='%Y%m%d')
    mentions["mentions_1"] = pd.to_datetime(mentions["mentions_1"], format='%Y%m%d%H%M%S')
    gkg["gkg_1"] = pd.to_datetime(gkg["gkg_1"], format='%Y%m%d%H%M%S')

    export_translation["export_translation_1"] = pd.to_datetime(export_translation["export_translation_1"], format='%Y%m%d')
    mentions_translation["mentions_translation_1"] = pd.to_datetime(mentions_translation["mentions_translation_1"], format='%Y%m%d%H%M%S')
    gkg_translation["gkg_translation_1"] = pd.to_datetime(gkg_translation["gkg_translation_1"], format='%Y%m%d%H%M%S')
    
    return export, mentions, gkg, export_translation, mentions_translation, gkg_translation

###################################################
############# MERGE TABLE #########################
###################################################

def merge_table_bis(export, mentions, mentions_translation):
    sub_mentions_translation = mentions_translation.loc[:,["mentions_translation_0", "mentions_translation_14"]]
    sub_mentions_translation["mentions_translation_14"] = sub_mentions_translation["mentions_translation_14"].apply(lambda x: x.split(";")[0].split(":")[1])
    
    mentions_mentions_translation = mentions.merge(sub_mentions_translation, left_on='mentions_0', right_on='mentions_translation_0', how='left')
    
    export_mentions_mentions_translation_joined = mentions_mentions_translation.merge(export, left_on="mentions_0", right_on="export_0", how='left')

    return export_mentions_mentions_translation_joined


###################################################
############# REQUETE 1 ###########################
###################################################

def requete_1(export_mentions_mentions_translation_joined):
    requete1 = export_mentions_mentions_translation_joined.loc[:,["mentions_0", "mentions_1", "export_53", "mentions_translation_14"]]
    requete1['day'] = requete1["mentions_1"].dt.day
    requete1['month'] = requete1["mentions_1"].dt.month
    requete1['year'] = requete1["mentions_1"].dt.year

    requete1.rename(columns={"mentions_0" : "id_event",
                            "mentions_1" : "datetime",
                            "export_53" : "country_code",
                            "mentions_translation_14" : "source_langue"}, inplace=True)
    
    # Drop all rows with full NaN values
    col = requete1.columns.tolist()
    requete1 = requete1.dropna(subset=col, how='all')
    
    requete1.to_csv(r'C:/HUGO/Ecole/Telecom Paris/COURS/INF_728_Base_de_donnees_non_relationnelles/GDELT Project/requete1.csv', index=False)
    
    return requete1

###################################################
############# REQUETE 2 ###########################
###################################################

def requete_2(export): 
    
    requete2 = export.loc[:,["export_0", "export_1", "export_53", "export_26"]]
    requete2['day'] = requete2["export_1"].dt.day
    requete2['month'] = requete2["export_1"].dt.month
    requete2['year'] = requete2["export_1"].dt.year

    requete2.rename(columns={"export_0" : "id_event",
                            "export_1" : "datetime",
                            "export_53" : "country_code",
                            "export_26" : "event_code"}, inplace=True)
    
    # Drop all rows with full NaN values
    col = requete2.columns.tolist()
    requete2 = requete2.dropna(subset=col, how='all')
    
    requete2.to_csv(r'C:/HUGO/Ecole/Telecom Paris/COURS/INF_728_Base_de_donnees_non_relationnelles/GDELT Project/requete2.csv', index=False)
    
    return requete2

###################################################
############# REQUETE 3 ###########################
###################################################

def requete_3(gkg):    
    requete3 = gkg.loc[:,["gkg_0","gkg_1", "gkg_3", "gkg_7", "gkg_11", "gkg_9", "gkg_15"]]

    requete3['day'] = requete3["gkg_1"].dt.day
    requete3['month'] = requete3["gkg_1"].dt.month
    requete3['year'] = requete3["gkg_1"].dt.year

    requete3.rename(columns={"gkg_0" : "id_gkg",
                             "gkg_1" : "datetime",
                            "gkg_3" : "source_domain",
                            "gkg_7" : "themes",
                            "gkg_11" : "persons", 
                            "gkg_9" : "locations",
                            "gkg_15" : "avg_tone"}, inplace=True)


    requete3["locations"] = requete3["locations"].apply(lambda x : str(x).split(",")[0].split("#")[-1])
    requete3["avg_tone"] = requete3["avg_tone"].apply(lambda x : float(str(x).split(",")[0]))
    
    # Drop all rows with full NaN values
    col = requete3.columns.tolist()
    requete3 = requete3.dropna(subset=col, how='all')

    requete3.to_csv(r'C:/HUGO/Ecole/Telecom Paris/COURS/INF_728_Base_de_donnees_non_relationnelles/GDELT Project/requete3.csv', index=False)

    return requete3

###################################################
############# REQUETE 4 ###########################
###################################################
    
def requete_4(gkg_translation):
    
    requete4 = gkg_translation.loc[:,["gkg_translation_0", "gkg_translation_7", "gkg_translation_11", "gkg_translation_9", "gkg_translation_15", "gkg_translation_25"]]

    requete4.rename(columns={"gkg_translation_0" : "id_gkg_translation",
                             "gkg_translation_7" : "themes",
                            "gkg_translation_11" : "persons", 
                            "gkg_translation_9" : "locations",
                            "gkg_translation_15" : "avg_tone",
                            "gkg_translation_25" : "source_langue"}, inplace=True)

    requete4 = requete4.replace(to_replace='None', value=np.nan).dropna()
    requete4["source_langue"] = requete4["source_langue"].apply(lambda x: x.split(";")[0].split(":")[1])
    requete4["locations"] = requete4["locations"].apply(lambda x : str(x).split(",")[0].split("#")[-1])
    requete4["avg_tone"] = requete4["avg_tone"].apply(lambda x : float(str(x).split(",")[0]))
    
    # Drop all rows with full NaN values
    col = requete4.columns.tolist()
    requete4 = requete4.dropna(subset=col, how='all')
    
    requete4.to_csv(r'C:/HUGO/Ecole/Telecom Paris/COURS/INF_728_Base_de_donnees_non_relationnelles/GDELT Project/requete4.csv', index=False)

    return requete4

def main():
  if len(sys.argv) != 4:
    print ('usage: ./preprocessing.py nb_url start_date end_date')
    sys.exit(1)

  arg1 = int(sys.argv[1])
  arg2 = str(sys.argv[2])
  arg3 = str(sys.argv[3])
  if len(sys.argv) == 4:
    start_time = time.time()
    
    print("\n#### SCRAPPING... #####\n")
    
    df = masterfilelist(arg1, arg2, arg3)
    
    df_translation = masterfilelist_translation(arg1, arg2, arg3)
    
    print("\n-------- %s seconde --------" % (time.time() - start_time))
    
    
    
    print("\n\n#### MERGING... #####\n")
    
    result = merge_table(df, df_translation)
    
    print("\n-------- %s seconde --------" % (time.time() - start_time))
    
    
    print("\n\n#### CLEANSING... #####\n")
    
    result = clean_dataset(result)
    
    print("\n-------- %s seconde --------" % (time.time() - start_time))
        
    
    
    print("\n\n#### CONCATENATING... #####\n")
    
    final = concat_table(result)
    
    print("\n-------- %s seconde --------" % (time.time() - start_time))
    
    
    print("\n\n#### READING & EXPORTING TO DATAFRAME... #####\n")
    
    export, mentions, gkg, translation_export, translation_mentions, translation_gkg = read_zip(final)
    
    print("-------- %s seconde --------" % (time.time() - start_time))

    print("\n\n#### Pre-processing base table... #####\n")
    
    export, mentions, gkg, export_translation, mentions_translation, gkg_translation = rename_columns(export, mentions, gkg, translation_export, translation_mentions, translation_gkg)

    print("\n\n#### Transforming date into datetime... #####\n")
    export, mentions, gkg, export_translation, mentions_translation, gkg_translation = date_to_datime(export, mentions, gkg, export_translation, mentions_translation, gkg_translation)

    
    print("\n\n#### Merging table... #####\n")
    export_mentions_mentions_translation_joined = merge_table_bis(export, mentions, mentions_translation) 

    print("\n\n#### Building Requete1... #####\n")
    requete_1(export_mentions_mentions_translation_joined)

    print("\n\n#### Building Requete2... #####\n")
    requete_2(export)
    
    print("\n\n#### Building Requete3... #####\n")
    requete_3(gkg)
    
    print("\n\n#### Building Requete4... #####\n")
    requete_4(gkg_translation)

  else:
    print ('unknown parameters: ') + arg1 + arg2 + arg3
    sys.exit(1)

if __name__ == '__main__':
  main()