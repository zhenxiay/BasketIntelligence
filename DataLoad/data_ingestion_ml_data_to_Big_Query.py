from BasketIntelligence.load_season_data import LoadSeasonData

def main():

    dataset = LoadSeasonData("2025",
                         "keen-vial-420113",
                         "BasketIntelligence")

    dataset.load_kmeans_team_shooting_to_big_query(table_name="kmeans_team_shooting",
                                                   n_cluster=5)

if __name__ == "__main__":
    main()
