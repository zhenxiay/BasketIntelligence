
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

        df_output = (df_output
                   .pipe(drop_rows)
                   .pipe(define_schema)                  
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
                   .pipe(filter_rows)                  
                   )

        return df_output