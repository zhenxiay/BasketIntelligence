
import pandas as pd

class CreateSeason():

    def __init__(self, year):
        self.year = year
        self.url_per_game = f'https://www.basketball-reference.com/leagues/NBA_{self.year}_per_game.html'
        self.url_adv_stats = f'https://www.basketball-reference.com/leagues/NBA_{self.year}_advanced.html'
        self.url_team_stats = f'https://www.basketball-reference.com/leagues/NBA_{self.year}.html'
    
    @staticmethod
    # Standard function which is going to be used by the read functions below for data cleaning
    def drop_summary_rows(X):
        X = X.drop(X[X.Team == 'League Average'].index)
        return X
        
    def read_team_shooting(self):
        list = pd.read_html(self.url_team_stats,
                          encoding = 'utf-8', 
                          decimal='.', 
                          thousands=',')
        df_output = list[11]
        
        # preperation for multiindex conversion for affected columns
        df_fg_attempts = df_output.pop('% of FGA by Distance')
        df_fg_pct = df_output.pop('FG% by Distance')
        df_corner_pct = df_output.pop('Corner')
        
        def convert_multi_index(X):

    		# drop first index level 
			X = X.droplevel(0, axis=1)

    		# add converted single index columns back to the output dataframe
			X = X.assign(**{f'{col}-FGA': df_fg_attempts[col] for col in df_fg_attempts.columns})
			X = X.assign(**{f'{col}-pct': df_fg_pct[col] for col in df_fg_pct.columns})
			X = X.assign(**{f'{col}-corner': df_corner_pct[col] for col in df_corner_pct.columns})
    
        	# define a function to turn columns for shoot attempts and pct per distance and corner from nested columns to single index columns
			selected_columns = X.filter(regex='(Team|-FGA|-pct|-corner)').columns
			X = X[selected_columns]

			return X
            
		def rename_columns(X):
			X.columns = X.columns.str.replace('%', '', regex=False)
            
			return X            
        
        # shape output dataframe with pipe and the functions defined above         
		df_output = (df_output
				.pipe(convert_multi_index)
				.pipe(self.drop_summary_rows)
				.pipe(rename_columns)                  
                   )

        return df_output
    
    def read_team_adv_stats(self):
        list = pd.read_html(self.url_team_stats,
                          encoding = 'utf-8', 
                          decimal='.', 
                          thousands=',')
        df_output = list[10]

        # preperation for multiindex conversion for affected columns
        df_off = df_output.pop('Offense Four Factors')
        df_dev = df_output.pop('Defense Four Factors')

        # define a function to turn columns for offensive and defensive four factors from nested columns to single index columns
        def convert_multi_index(X):
            # define columns that are to be converted 
            cols_dev = ['eFG%','TOV%','DRB%','FT/FGA']
            cols_off = ['eFG%','TOV%','ORB%','FT/FGA']

            # drop first index level 
            X = X.droplevel(0, axis=1)

            # add converted single index columns back to the output dataframe
            X = X.assign(**{f'dev_{col}': df_dev[col] for col in cols_dev})
            X = X.assign(**{f'off_{col}': df_off[col] for col in cols_off})

            return X

        def drop_na_columns(X):
            X = X.dropna(how='all',axis=1)

            return X

        def rename_columns(X):
            column_mapping = {'Attend.': 'Attend',
                              'Attend./G': 'Attend_G'
                              }
            
            X.rename(columns=column_mapping, 
                     inplace=True)

            X.columns = X.columns.str.replace('%', '_pct', regex=False)
            X.columns = X.columns.str.replace('/', '_', regex=False)

            return X

        df_output = (df_output
                   .pipe(convert_multi_index)
                   .pipe(drop_na_columns)
                   .pipe(self.drop_summary_rows)
                   .pipe(rename_columns)                  
                   )

        return df_output
    
    def read_stats_per_game(self):
        list = pd.read_html(self.url_per_game,
                          encoding = 'utf-8', 
                          decimal='.', 
                          thousands=',')
        df_output = list[0]

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
                              'FG%': 'FG_pct',
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
        list = pd.read_html(self.url_adv_stats,
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

        df_output = list[0]

        df_output = (df_output
                   .pipe(drop_rows)
                   .pipe(define_schema)
                   .pipe(rename_columns)
                   .pipe(filter_rows)                  
                   )

        return df_output