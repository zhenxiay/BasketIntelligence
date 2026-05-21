'''
This script can be executed by AI agent to perform the create game report task defined in the Agent Skill.
'''
import os
os.environ["NO_PROXY"] = "localhost, 127.0.0.1"
os.environ["no_proxy"] = "localhost, 127.0.0.1"

import pandas as pd
import pandas_toon

import typer
app = typer.Typer()

@app.command()
def get_game_report(
    date: str = typer.Option(
        "20251116", 
        help="Date of the game."
        ),
    home_team: str = typer.Option(
        "HOU", 
        help="Abbreviation of the home team (e.g., LAL for Los Angeles Lakers)."
        ),
    away_team: str = typer.Option(
        "ORL", 
        help="Abbreviation of the away team (e.g., BOS for Boston Celtics)."
        )
):
    """
    Get the report for a given game.
    """
    
    url=f"https://www.basketball-reference.com/boxscores/pbp/{date}0{home_team}.html"

    cols = [2,4]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    df = pd.read_html(url, storage_options=headers)[0].droplevel(0, axis=1)

    print(df.to_toon())

if __name__ == "__main__":
    app()