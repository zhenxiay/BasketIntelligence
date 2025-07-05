from sklearn.cluster import KMeans
import pandas as pd
from BasketIntelligence.create_season import CreateSeason

def k_means_team_shooting_clustering(year, n_clusters):
    '''
    use kmeans to cluster teams based on their shooting stats
    '''

    df_input = CreateSeason(year).read_team_shooting()
    df_output = pd.DataFrame(df_input.pop('Team'), columns=['Team'])

    k_means = KMeans(init="k-means++",
                     n_clusters=n_clusters,
                     n_init=10)
    k_means.fit(df_input)

    pred = k_means.predict(df_input)

    df_output['kMeans'] = pred

    return df_output

def k_means_player_clustering(year, n_clusters):
    '''
    use kmeans to cluster players based on selected advanced stats
    '''

    dataset = CreateSeason(year).read_adv_stats().drop(columns=['Awards'])

    k_means_cols = ['TS','3PAr','FTr', 'ORB_rate', 'DRB_rate', 'AST_rate', 'STL_rate', 'BLK_rate', 'TOV_rate', 'USG_rate']
    output_cols = ['Rk','Player','Pos','Age','Team', 'kMeans']

    df_input = dataset[k_means_cols]
    df_input = df_input.dropna(how='any')
    dataset = dataset.dropna(how='any')

    k_means = KMeans(init="k-means++",
                     n_clusters=n_clusters,
                     n_init=10)
    k_means.fit(df_input)

    pred = k_means.predict(df_input)

    dataset['kMeans'] = pred

    df_output = dataset[output_cols]

    return df_output
