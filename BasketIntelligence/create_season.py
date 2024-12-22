
import pandas as pd

class CreateSeason():

    def __init__(self, year):
        self.year = year
        self.url_per_game = f'https://www.basketball-reference.com/leagues/NBA_{self.year}_per_game.html'
        self.url_adv_stats = f'https://www.basketball-reference.com/leagues/NBA_{self.year}_advanced.html'
    
    def read_stats_per_game(self):
        df = pd.read_html(self.url_per_game,
                          encoding = 'utf-8', 
                          decimal='.', 
                          thousands=',')
        df_output = df[0]

        def drop_rows(X):
            X = X.drop(X[X.Player == 'Player'].index)
            X = X.drop(X[X.Player == 'League Average'].index)
            return X

        def define_schema(X):
            X = X.astype({
                            'Rk': 'int',
                            'G': 'int',
                            'GS': 'int',
                            'Age': 'int',  
                            'MP': 'float',
                            'FG': 'float',
                            'FGA': 'float',
                            '3P': 'float', 
                            '3PA': 'float',
                            '3P%': 'float',
                            '2P': 'float',
                            '2PA': 'float',
                            '2P%': 'float',
                            'eFG%': 'float',
                            'FT': 'float',
                            'FTA': 'float',
                            'FT%': 'float',
                            'ORB': 'float', 
                            'DRB': 'float', 
                            'TRB': 'float', 
                            'AST': 'float', 
                            'STL': 'float', 
                            'BLK': 'float', 
                            'TOV': 'float',
                            'PF': 'float', 
                            'PTS': 'float'
                          })
            return X
        
        def rename_columns(X):
            column_mapping = {'3P%': '3P_pct',
                              'eFG%': 'eFG_pct',
                              'FT%': 'FT_pct',
                              '2P%': '2P_pct'
                              }
            
            X.rename(columns=column_mapping, 
                     inplace=True)
            return X

        df_output = (df_output
                   .pipe(drop_rows)
                   .pipe(define_schema)
                   .pipe(rename_columns)                  
                   )

        return df_output

    def read_adv_stats(self):
        df = pd.read_html(self.url_adv_stats,
                          encoding = 'utf-8', 
                          decimal='.', 
                          thousands=',')

        def define_schema(X):
            X = X.astype({
                            'Rk': 'int',
                            'G': 'int',
                            'GS': 'int',
                            'Age': 'int',  
                            'MP': 'float',
                            'PER': 'float',
                            'TS%': 'float',
                            '3PAr': 'float', 
                            'FTr': 'float',
                            'ORB%': 'float',
                            'DRB%': 'float',
                            'TRB%': 'float',
                            'AST%': 'float',
                            'STL%': 'float',
                            'BLK%': 'float',
                            'TOV%': 'float',
                            'USG%': 'float',
                            'OWS': 'float', 
                            'DWS': 'float', 
                            'WS': 'float', 
                            'WS/48': 'float', 
                            'OBPM': 'float', 
                            'DBPM': 'float', 
                            'BPM': 'float', 
                            'VORP': 'float'
                          })
            return X

        
        def rename_columns(X):
            column_mapping = {'WS/48': 'WS_48',
                              'TS%': 'TS',
                              'ORB%': 'ORB_rate',
                              'DRB%': 'ORD_rate',
                              'TRB%': 'TRB_rate',
                              'AST%': 'AST_rate',
                              'STL%': 'STL_rate',
                              'BLK%': 'BLK_rate',
                              'TOV%': 'TOV_rate',
                              'USG%': 'USG_rate'}
            
            X.rename(columns=column_mapping, 
                     inplace=True)
            return X
        
        def drop_rows(X):
            X = X.drop(X[X.Player == 'Player'].index)
            X = X.drop(X[X.Player == 'League Average'].index)
            return X
        
        def filter_rows(X):
            X = X[X['MP'] > 100]
            return X

        df_output = df[0]

        df_output = (df_output
                   .pipe(drop_rows)
                   .pipe(define_schema)
                   .pipe(rename_columns)
                   .pipe(filter_rows)                  
                   )

        return df_output