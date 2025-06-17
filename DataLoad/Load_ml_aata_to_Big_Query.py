from BasketIntelligence.load_season import LoadSeasonData

def main():

    dataset = LoadSeasonData("2025",
                         "keen-vial-420113",
                         "BasketIntelligence")

    dataset.load_kmeans_team_shooting_to_big_query("kmeans_team_shooting")

if __name__ == "__main__":
    main()
