
import pandas as pd
from BasketIntelligence.utils.logger import get_logger

class CreateSeason():
    '''
    This class is used to create a data object for a specific NBA season.
    This object contains methods to read and process data from basketball-reference.com.
    '''

    def __init__(self, year):
        self.year = year
        self.base_url = 'https://www.basketball-reference.com/leagues/NBA'
        self.url_per_game = f'{self.base_url}_{self.year}_per_game.html'
        self.url_adv_stats = f'{self.base_url}_{self.year}_advanced.html'
        self.url_team_stats = f'{self.base_url}_{self.year}.html'
        self.logger = get_logger()

    @staticmethod
    def drop_summary_rows(df_input):
        '''
        Standard function which is used by the read functions below for data cleaning
        '''
        try:
            df_input = df_input.drop(df_input[df_input.Team == 'League Average'].index)
        except AttributeError:
            df_input = df_input.drop(df_input[df_input.Tm == 'League Average'].index)
        return df_input

    @staticmethod
    def add_season_number(df_input, season):
        '''
        Standard function which is used to add the season number to the dataframe
        '''
        df_input['Season'] = season
        return df_input

    def get_dataframe_list(self, url):
        '''
        This function returns a list of dataframes for html page.
        '''
        list = pd.read_html(url,
                            encoding='utf-8',
                            decimal='.',
                            thousands=',')
        return list

    def read_team_shooting(self):
        '''
        This function reads and converts the team shooting stats from the data source.
        '''
        list = self.get_dataframe_list(url=self.url_team_stats)

        df_output = list[11]

        # preperation for multiindex conversion for affected columns
        df_fg_attempts = df_output.pop('% of FGA by Distance')
        df_fg_pct = df_output.pop('FG% by Distance')
        df_corner_pct = df_output.pop('Corner')

        def convert_multi_index(df_input):

    		# drop first index level
            df_input = df_input.droplevel(0, axis=1)

    		# add converted single index columns back to the output dataframe
            df_input = df_input.assign(**{f'FGA-{col}': df_fg_attempts[col] for col in df_fg_attempts.columns})
            df_input = df_input.assign(**{f'pct-{col}': df_fg_pct[col] for col in df_fg_pct.columns})
            df_input = df_input.assign(**{f'corner-{col}': df_corner_pct[col] for col in df_corner_pct.columns})

            # turn following columns from nested columns to single index columns:
            # shoot attempts, pct per distance and corner
            selected_columns = df_input.filter(regex='(Team|FGA-|pct-|corner-)').columns
            df_input = df_input[selected_columns]

            return df_input

        def rename_columns(df_input):
            df_input.columns = df_input.columns.str.replace('%', '', regex=False)
            df_input.columns = df_input.columns.str.replace('-', '_', regex=False)

            return df_input

        # shape output dataframe with pipe and the functions defined above
        df_output = (df_output
                .pipe(convert_multi_index)
                .pipe(self.drop_summary_rows)
                .pipe(rename_columns)
                .pipe(self.add_season_number, season = self.year)
                )

        self.logger.info(f"Dataframe created with {len(df_output)} rows...")

        return df_output

    def read_team_adv_stats(self):
        '''
        This function reads and converts the team advanced stats from the data source.
        '''
        list = self.get_dataframe_list(url=self.url_team_stats)

        df_output = list[10]

        # preperation for multiindex conversion for affected columns
        df_off = df_output.pop('Offense Four Factors')
        df_dev = df_output.pop('Defense Four Factors')

        def convert_multi_index(df_input):
            # define a function to turn following columns from nested columns to single index columns:
            # for offensive and defensive four factors
            # define columns that are to be converted
            cols_dev = ['eFG%','TOV%','DRB%','FT/FGA']
            cols_off = ['eFG%','TOV%','ORB%','FT/FGA']

            # drop first index level
            df_input = df_input.droplevel(0, axis=1)

            # add converted single index columns back to the output dataframe
            df_input = df_input.assign(**{f'dev_{col}': df_dev[col] for col in cols_dev})
            df_input = df_input.assign(**{f'off_{col}': df_off[col] for col in cols_off})

            return df_input

        def drop_na_columns(df_input):
            df_input = df_input.dropna(how='all',axis=1)

            return df_input

        def rename_columns(df_input):
            column_mapping = {'Attend.': 'Attend',
                              'Attend./G': 'Attend_G'
                              }

            df_input.rename(columns=column_mapping,
                     inplace=True)

            df_input.columns = df_input.columns.str.replace('%', '_pct', regex=False)
            df_input.columns = df_input.columns.str.replace('/', '_', regex=False)

            return df_input

        df_output = (df_output
                   .pipe(convert_multi_index)
                   .pipe(drop_na_columns)
                   .pipe(self.drop_summary_rows)
                   .pipe(rename_columns)
                   .pipe(self.add_season_number, season = self.year)
                   )

        self.logger.info(f"Dataframe created with {len(df_output)} rows...")

        return df_output

    def read_stats_per_game(self):
        '''
        This function reads and converts the player per game stats from the data source.
        '''
        list = self.get_dataframe_list(url=self.url_per_game)

        df_output = list[0]

        def drop_rows(df_input):
            df_input = df_input.drop(df_input[df_input.Player == 'Player'].index)
            df_input = df_input.drop(df_input[df_input.Player == 'League Average'].index)
            return df_input

        def define_schema(df_input):
            df_input = df_input.astype({
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
            return df_input

        def rename_columns(df_input):
            column_mapping = {'3P%': '3P_pct',
                              'FG%': 'FG_pct',
                              'eFG%': 'eFG_pct',
                              'FT%': 'FT_pct',
                              '2P%': '2P_pct'
                              }

            df_input.rename(columns=column_mapping,
                     inplace=True)
            return df_input

        df_output = (df_output
                   .pipe(drop_rows)
                   .pipe(define_schema)
                   .pipe(rename_columns)
                   .pipe(self.add_season_number, season = self.year)
                   )

        self.logger.info(f"Dataframe created with {len(df_output)} rows...")

        return df_output

    def read_adv_stats(self):
        '''
        This function reads and converts the player advanced stats from the data source.
        '''
        list = self.get_dataframe_list(url=self.url_adv_stats)

        def define_schema(df_input):
            df_input = df_input.astype({
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
            return df_input


        def rename_columns(df_input):
            column_mapping = {'WS/48': 'WS_48',
                              'TS%': 'TS',
                              'ORB%': 'ORB_rate',
                              'DRB%': 'DRB_rate',
                              'TRB%': 'TRB_rate',
                              'AST%': 'AST_rate',
                              'STL%': 'STL_rate',
                              'BLK%': 'BLK_rate',
                              'TOV%': 'TOV_rate',
                              'USG%': 'USG_rate'}

            df_input.rename(columns=column_mapping,
                     inplace=True)
            return df_input

        def drop_rows(df_input):
            df_input = df_input.drop(df_input[df_input.Player == 'Player'].index)
            df_input = df_input.drop(df_input[df_input.Player == 'League Average'].index)
            return df_input

        def filter_rows(df_input):
            df_input = df_input[df_input['MP'] > 100]
            return df_input

        df_output = list[0]

        df_output = (df_output
                   .pipe(drop_rows)
                   .pipe(define_schema)
                   .pipe(rename_columns)
                   .pipe(filter_rows)
                   .pipe(self.add_season_number, season = self.year)
                   )

        self.logger.info(f"Dataframe created with {len(df_output)} rows...")

        return df_output
