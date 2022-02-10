# -*- coding: utf-8 -*-


#######################################
#   Prepocessing GDELT Project
#######################################

#######################################
#   Import libraries
#######################################

import validators
import requests
import pandas as pd
import numpy as np
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import datetime
import time
import sys
# import csv

# csv.field_size_limit(sys.maxsize)

#######################################
#   SCRAPPING
#######################################

# URL : masterfilelist.txt


def masterfilelist(nb_url, start_date, end_date):

    response = requests.get("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt")
    content = response.content.decode("utf-8") 
    liens = content.split('\n')[-nb_url:]

    liste = list()
    for i in liens:
        liste.append(i.split(" ")[-1])

    df = pd.DataFrame(liste, columns=['url'])
    df['date_str'] = df['url'].apply(lambda x: x.split("/")[-1].split(".")[0][0:12])
    df = df.iloc[:df.shape[0]-1, :]
    df["date"] = pd.to_datetime(df["date_str"], format='%Y%m%d%H%M')

    start_datem = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    end_datem = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
    df = df.loc[(df['date'] >= start_datem) & (df['date'] <= end_datem)]
    
    df['type_csv'] = df['url'].apply(lambda x: x.lower().split(".csv")[0].split(".")[-1])

    df['id'] = df['date_str']+'_'+df['type_csv']
    
    df = df.drop(columns=['date'])
    
    return df

# URL : masterfilelist-translation.txt


def masterfilelist_translation(nb_url, start_date, end_date):

    response = requests.get("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt")
    content = response.content.decode("utf-8") 
    liens = content.split('\n')[-nb_url:]

    liste = list()
    for i in liens:
        liste.append(i.split(" ")[-1])

    df = pd.DataFrame(liste, columns=['url_translation'])
    df['date_str_translation'] = df['url_translation'].apply(lambda x: x.split("/")[-1].split(".")[0][0:12])
    df = df.iloc[:df.shape[0]-1, :]
    df["date"] = pd.to_datetime(df["date_str_translation"], format='%Y%m%d%H%M')

    start_datem = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    end_datem = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
    df = df.loc[(df['date'] >= start_datem) & (df['date'] <= end_datem)]
    
    df['type_csv_translation'] =\
        df['url_translation'].apply(lambda x: '_'.join(x.lower().split(".csv")[0].split(".")[-2:]))

    df['type_csv'] = df['url_translation'].apply(lambda x: x.lower().split(".csv")[0].split(".")[-1])

    df['id'] = df['date_str_translation']+'_'+df['type_csv']

    df = df.drop(columns=['type_csv', 'date'])
    
    return df

###################################################
#   VERIFIFY URL
###################################################


def verify_url(u):
    if validators.url(u):
        return True
    else:
        return False
    
# Merge masterfile.txt and masterfile_translation.txt


def merge_table(df, df_translation):
    
    # - left join des tableaux
    # - Première séléction : Supprimons les lignes où des NaN apparait
    # - Vérification URL
    
    result = df.merge(df_translation, on='id', how='left').dropna(axis='rows')
    result['work'] = result['url'].apply(lambda x: verify_url(x))
    result['work_translation'] = result['url_translation'].apply(lambda x: verify_url(x))
    return result

###################################################
#   CLEAN DATASET
###################################################


def clean_dataset(df):
    
    dk = df.groupby('date_str').count()[['id']]
    liste = dk[dk.id < 3].index.tolist()
    for item in liste:
        df = df.loc[df['date_str'] != item]

    liste = df.loc[df['work'] == False]['date_str'].unique().tolist()
    for item in liste:
        df = df.loc[df['date_str'] != item]

    liste = df.loc[df['work_translation'] == False]['date_str'].unique().tolist()
    for item in liste:
        df = df.loc[df['date_str'] != item]
            
    return df


###################################################
#   FUSION TABLE
###################################################

def concat_table(result):
    
    # Séparation des données de base et de translation ET concaténation
    df_base = result[['url', 'type_csv']]
    df_translation = result[['url_translation', 'type_csv_translation']]
    df_translation = df_translation.rename(columns={'url_translation': "url", 'type_csv_translation': "type_csv"})
    final = pd.concat([df_base, df_translation])
    
    return final

###################################################
#   LECTURE ZIPS
###################################################


def read_zip(final):
    
    export = final.loc[final['type_csv'] == 'export', 'url']
    mentions = final.loc[final['type_csv'] == 'mentions', 'url']
    gkg = final.loc[final['type_csv'] == 'gkg', 'url']
    translation_export = final.loc[final['type_csv'] == 'translation_export', 'url']
    translation_mentions = final.loc[final['type_csv'] == 'translation_mentions', 'url']
    translation_gkg = final.loc[final['type_csv'] == 'translation_gkg', 'url']
    
    df_export = list()
    df_mentions = list()
    df_gkg = list()
    df_translation_export = list()
    df_translation_mentions = list()
    df_translation_gkg = list()
    
    for i in export.tolist():
        url = urlopen(i) 
        k = i.split("/")[-1].split(".zip")[0]
        zipfile = ZipFile(BytesIO(url.read()))
        ffdata = pd.read_csv(zipfile.open(k),
                             header=None,
                             sep="\t",
                             engine='python',
                             encoding='latin-1',
                             on_bad_lines='skip')
        df_export.append(ffdata)
        
    for i in mentions.tolist():
        url = urlopen(i) 
        k = i.split("/")[-1].split(".zip")[0]
        zipfile = ZipFile(BytesIO(url.read()))
        ffdata = pd.read_csv(zipfile.open(k),
                             header=None,
                             sep="\t",
                             engine='python',
                             encoding='latin-1',
                             on_bad_lines='skip')
        df_mentions.append(ffdata)
        
    for i in gkg.tolist():
        url = urlopen(i) 
        k = i.split("/")[-1].split(".zip")[0]
        zipfile = ZipFile(BytesIO(url.read()))
        ffdata = pd.read_csv(zipfile.open(k),
                             header=None,
                             sep="\t",
                             engine='python',
                             encoding='latin-1',
                             on_bad_lines='skip')
        df_gkg.append(ffdata)
        
    for i in translation_export.tolist():
        url = urlopen(i) 
        k = i.split("/")[-1].split(".zip")[0]
        zipfile = ZipFile(BytesIO(url.read()))
        ffdata = pd.read_csv(zipfile.open(k),
                             header=None,
                             sep="\t",
                             engine='python',
                             encoding='latin-1',
                             on_bad_lines='skip')
        df_translation_export.append(ffdata)
        
    for i in translation_mentions.tolist():
        url = urlopen(i) 
        k = i.split("/")[-1].split(".zip")[0]
        zipfile = ZipFile(BytesIO(url.read()))
        ffdata = pd.read_csv(zipfile.open(k),
                             header=None,
                             sep="\t",
                             engine='python',
                             encoding='latin-1',
                             on_bad_lines='skip')
        df_translation_mentions.append(ffdata)
        
    for i in translation_gkg.tolist():
        url = urlopen(i) 
        k = i.split("/")[-1].split(".zip")[0]
        zipfile = ZipFile(BytesIO(url.read()))
        ffdata = pd.read_csv(zipfile.open(k),
                             header=None,
                             sep="\t",
                             engine='python',
                             encoding='latin-1',
                             on_bad_lines='skip')
        df_translation_gkg.append(ffdata)
        
    export = pd.concat(df_export)
    mentions = pd.concat(df_mentions)
    gkg = pd.concat(df_gkg)

    export_translation = pd.concat(df_translation_export)
    mentions_translation = pd.concat(df_translation_mentions)
    gkg_translation = pd.concat(df_translation_gkg)
        
    return export, mentions, gkg, export_translation, mentions_translation, gkg_translation


###################################################
# PRE PROCESSING BASE TABLE
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
#   DATE TO DATETIME
###################################################


def date_to_datime(export, mentions, gkg, export_translation, mentions_translation, gkg_translation):
    export["export_1"] = pd.to_datetime(export["export_1"],
                                        format='%Y%m%d')
    mentions["mentions_1"] = pd.to_datetime(mentions["mentions_1"],
                                            format='%Y%m%d%H%M%S')
    gkg["gkg_1"] = pd.to_datetime(gkg["gkg_1"],
                                  format='%Y%m%d%H%M%S')

    export_translation["export_translation_1"] = pd.to_datetime(export_translation["export_translation_1"],
                                                                format='%Y%m%d')
    mentions_translation["mentions_translation_1"] = pd.to_datetime(mentions_translation["mentions_translation_1"],
                                                                    format='%Y%m%d%H%M%S')
    gkg_translation["gkg_translation_1"] = pd.to_datetime(gkg_translation["gkg_translation_1"],
                                                          format='%Y%m%d%H%M%S')
    
    return export, mentions, gkg, export_translation, mentions_translation, gkg_translation

###################################################
#   MERGE TABLE
###################################################


def merge_table_bis(export, mentions, mentions_translation):
    sub_mentions_translation =\
        mentions_translation.loc[:, ["mentions_translation_0", "mentions_translation_14"]]
    sub_mentions_translation["mentions_translation_14"] =\
        sub_mentions_translation["mentions_translation_14"].apply(lambda x: x.split(";")[0].split(":")[1])
    mentions_mentions_translation =\
        mentions.merge(sub_mentions_translation, left_on='mentions_0', right_on='mentions_translation_0', how='left')
    export_mentions_mentions_translation_joined =\
        mentions_mentions_translation.merge(export, left_on="mentions_0", right_on="export_0", how='left')
    return export_mentions_mentions_translation_joined


###################################################
#   REQUETE 1
###################################################

def requete_1(export_mentions_mentions_translation_joined):
    requete1 = export_mentions_mentions_translation_joined.loc[:, ["mentions_0", 
                                                                   "mentions_1", 
                                                                   "export_53", 
                                                                   "export_5",  
                                                                   "export_15", 
                                                                   "export_34", 
                                                                   "mentions_translation_14"]]
    requete1['date'] = requete1["mentions_1"].dt.date
    requete1['day'] = requete1["mentions_1"].dt.day
    requete1['month'] = requete1["mentions_1"].dt.month
    requete1['year'] = requete1["mentions_1"].dt.year
    

    requete1.rename(columns={"mentions_0" : "id_event",
                            "mentions_1" : "datetime",
                            "export_53" : "country_code",
                            "export_5" : "actor1_code",
                            "export_15" : "actor2_code",
                            "export_34" : "avg_tone",
                            "mentions_translation_14" : "source_langue"}, inplace=True)
    
    # Drop all rows with full NaN values
    col = requete1.columns.tolist()
    requete1 = requete1.dropna(subset=col, how='all')
    
    # We select all rows with non-NaN values for column "country_code"
    requete1 = requete1[requete1['country_code'].notna()]

    # requete1.to_csv('out/requete1.csv', index=False)
    requete1.to_csv(r'/home/ubuntu/csv/requete1/requete1.csv', index=False)
    
    return requete1

###################################################
#   REQUETE 2
###################################################


def requete_2(export):
    
    requete2 = export.loc[:, ["export_0", "export_1", "export_53", "export_31"]]
    
    requete2['date'] = requete2["export_1"].dt.date
    requete2['day'] = requete2["export_1"].dt.day
    requete2['month'] = requete2["export_1"].dt.month
    requete2['year'] = requete2["export_1"].dt.year

    requete2.rename(columns={"export_0": "id_event",
                             "export_1": "datetime",
                             "export_53": "country_code",
                             "export_31": "nb_mentions"}
                             ,inplace=True)
    
    # Drop all rows with full NaN values
    col = requete2.columns.tolist()
    requete2 = requete2.dropna(subset=col, how='all')
    requete2['country_code'].fillna('NO_INFO', inplace=True)

    # requete2.to_csv('out/requete2.csv', index=False)
    requete2.to_csv(r'/home/ubuntu/csv/requete2/requete2.csv', index=False)
    
    return requete2

###################################################
#   REQUETE 3
###################################################


def requete_3(gkg):
    requete3 = gkg.loc[:, ["gkg_0", "gkg_1", "gkg_3", "gkg_7", "gkg_11", "gkg_9", "gkg_15"]]
    
    requete3['date'] = requete3["gkg_1"].dt.date
    requete3['day'] = requete3["gkg_1"].dt.day
    requete3['month'] = requete3["gkg_1"].dt.month
    requete3['year'] = requete3["gkg_1"].dt.year

    requete3.rename(columns={"gkg_0": "id_gkg",
                             "gkg_1": "datetime",
                             "gkg_3": "source_domain",
                             "gkg_7": "themes",
                             "gkg_11": "persons",
                             "gkg_9": "locations",
                             "gkg_15": "avg_tone"}, inplace=True)

    requete3["locations"].fillna('NO_INFO', inplace=True)
    requete3["locations"] = requete3["locations"].apply(lambda x: str(x).split(",")[0].split("#")[-1])
    requete3["avg_tone"].fillna('0.0', inplace=True)
    requete3["avg_tone"] = requete3["avg_tone"].apply(lambda x: float(str(x).split(",")[0]))
    
    # Drop url as id_gkg
    requete3 = requete3[requete3["id_gkg"].apply(lambda x: x[0].isnumeric())]        
    
    # Drop all rows with full NaN values
    col = requete3.columns.tolist()
    requete3 = requete3.dropna(subset=col, how='all')

    # requete3.to_csv('out/requete3.csv', index=False)
    requete3.to_csv(r'/home/ubuntu/csv/requete3/requete3.csv', index=False)

    return requete3

def main():
    if len(sys.argv) != 4:
        print('usage: ./preprocessing.py nb_url start_date end_date')
        sys.exit(1)
    arg1 = int(sys.argv[1])
    arg2 = str(sys.argv[2])
    arg3 = str(sys.argv[3])
    if len(sys.argv) == 4:
        start_time = time.time()
        print("#### SCRAPING... #####")
        df = masterfilelist(arg1, arg2, arg3)
        df_translation = masterfilelist_translation(arg1, arg2, arg3)
        print("-------- %s secondes --------" % (round(time.time() - start_time, 2)))
        print("#### MERGING... #####")
        result = merge_table(df, df_translation)
        print("-------- %s secondes --------" % (round(time.time() - start_time, 2)))
        print("#### CLEANING... #####")
        result = clean_dataset(result)
        print("-------- %s secondes --------" % (round(time.time() - start_time, 2)))
        print("#### CONCATENATING... #####")
        final = concat_table(result)
        print("-------- %s secondes --------" % (round(time.time() - start_time, 2)))
        print("#### READING & EXPORTING TO DATAFRAME... #####")
        export, mentions, gkg, translation_export, translation_mentions, translation_gkg =\
            read_zip(final)
        print("-------- %s secondes --------" % (round(time.time() - start_time, 2)))
        print("#### Pre-processing base table... #####")
        export, mentions, gkg, export_translation, mentions_translation, gkg_translation =\
            rename_columns(export, mentions, gkg, translation_export, translation_mentions, translation_gkg)
        print("#### Transforming date into datetime... #####")
        export, mentions, gkg, export_translation, mentions_translation, gkg_translation =\
            date_to_datime(export, mentions, gkg, export_translation, mentions_translation, gkg_translation)
        print("#### Merging table... #####")
        export_mentions_mentions_translation_joined = merge_table_bis(export, mentions, mentions_translation)
        print("#### Building Requete1... #####")
        requete_1(export_mentions_mentions_translation_joined)
        print("#### Building Requete2... #####")
        requete_2(export)
        print("#### Building Requete3... #####")
        requete_3(gkg)
        print("--- Temps total : %s secondes" % (round(time.time() - start_time, 2)))
    else:
        print('unknown parameters: ' + str(arg1) + arg2 + arg3)
        sys.exit(1)


if __name__ == '__main__':
    main()
