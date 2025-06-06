from BasketIntelligence.create_season import CreateSeason
from sklearn.cluster import KMeans
import pandas as pd

def k_means_team_shooting_clustering(year, n_clusters):

    X = CreateSeason(year).read_team_shooting()
    df_output = pd.DataFrame(X.pop('Team'), columns=['Team'])

    k_means = KMeans(init="k-means++", 
                     n_clusters=n_clusters, 
                     n_init=10)
    k_means.fit(X)

    pred = k_means.predict(X)

    df_output['kMeans'] = pred

    return df_output

def k_means_player_clustering(year, n_clusters):
  
    dataset = CreateSeason(year).read_adv_stats().drop(columns=['Awards'])
        
    label_cols = ['Rk','Player','Pos','Age','Team']
    k_means_cols = ['TS_rate','3PAr','FTr', 'ORB_rate', 'DRB_rate', 'AST_rate', 'STL_rate', 'BLK_rate', 'TOV_rate', 'USG_rate']
    output_cols = ['Rk','Player','Pos','Age','Team', 'kMeans']
  
    X = dataset[k_means_cols]
    X = X.dropna(how='any')
    dataset = dataset.dropna(how='any')

    k_means = KMeans(init="k-means++", 
                     n_clusters=n_clusters, 
                     n_init=10)
    k_means.fit(X)

    pred = k_means.predict(X)

    dataset['kMeans'] = pred

    df_output = dataset[output_cols]

    return df_output
  
