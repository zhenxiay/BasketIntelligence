
from BasketIntelligence.load_season_data import LoadSeasonData
import argparse

def main(params):

    loader = LoadSeasonData(params.year,
                            params.GCP_Name,
                            params.dataset)
                            
    loader.load_data(
        data_source=params.data_source,
        db_type='big_query',
        table_name=params.table_name
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description ='Define args for GitHub actions')

    parser.add_argument('year',type=str,help='Year of the NBA season.')
    parser.add_argument('GCP_Name',type=str, help='Google cloud project name.')
    parser.add_argument('dataset', type=str, help='Google Big Query dataset name.')
    parser.add_argument('data_source',type=str, help='Type of data that is to be loaded (per game, team shooting, adv stats etc.)')
    parser.add_argument('table_name',type=str, help='Name of the table in big query.')

    args = parser.parse_args()

    main(args)