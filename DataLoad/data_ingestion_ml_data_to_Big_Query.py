from BasketIntelligence.load_season_data import LoadSeasonData
import argparse

parser = argparse.ArgumentParser(description ='Define args for GitHub actions')

parser.add_argument('year',
                    type = str,
                    help ='Year of the NBA season.')

parser.add_argument('GCP_Name',
                   type = str,
                   help ='Google cloud project name.')

parser.add_argument('dataset',
                   type = str,
                   help ='Google Big Query dataset name.')

parser.add_argument('table_name',
                   type = str,
                   help ='Google Big Query table name to which the data will be ingested to.')

parser.add_argument('n_cluster',
                   type = str,
                   help ='Number of kmeans clusters that is to be applied.')

args = parser.parse_args()

def main():

    dataset = LoadSeasonData(args.year,
                             args.GCP_Name,
                             args.dataset)

    dataset.load_kmeans_team_shooting_to_big_query(
                                                   table_name=args.table_name,
                                                   n_cluster=int(args.n_cluster)
                                                  )

if __name__ == "__main__":
    main()
