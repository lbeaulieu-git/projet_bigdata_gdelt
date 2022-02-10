import pandas as pd
import warnings
warnings.filterwarnings("ignore")

requete3 = pd.read_csv('out/requete3.csv', sep=',')

def cell_split(col_split):
    df1 = requete3[['id_gkg', col_split]]
    if col_split == 'themes':
        df1.themes = df1.themes.apply(lambda x: str(x)[:-1])
    df1 = df1.fillna('NO_INFO')
    df1 = pd.concat(
        [pd.Series(row['id_gkg'], row[col_split].split(';'))
         for _, row in df1.iterrows()]).reset_index()
    df1.rename(columns={'index': col_split, 0: 'id_gkg'}, inplace=True)
    df1 = df1[['id_gkg', col_split]]
    return df1

df_tmp = cell_split('themes').merge(requete3.drop(columns={'themes'}), on='id_gkg', how='left')
dff = cell_split('persons').merge(df_tmp.drop(columns={'persons'}), on='id_gkg', how='left')

requete31 = dff[['locations', 'id_gkg', 'date', 'source_domain', 'avg_tone']].drop_duplicates()
requete32 = dff[['persons', 'id_gkg', 'date', 'source_domain', 'avg_tone']].drop_duplicates()
requete33 = dff[['themes', 'id_gkg', 'date', 'source_domain', 'avg_tone']].drop_duplicates()

requete31.to_csv('/home/ubuntu/csv/requete3/requete31.csv', index=False)
requete32.to_csv('/home/ubuntu/csv/requete3/requete32.csv', index=False)
requete33.to_csv('/home/ubuntu/csv/requete3/requete33.csv', index=False)

print("Done")
