'''
This is an example to demonstrate the game report tool with Agno agent.
'''
import os
os.environ["NO_PROXY"] = "localhost, 127.0.0.1"
os.environ["no_proxy"] = "localhost, 127.0.0.1"

import asyncio
from dotenv import load_dotenv
load_dotenv()
llm = os.getenv("llm", "OpenAI")

import sys
# Add src to path to import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd
import pandas-toon

from agno.agent import Agent
from agno.tools import tool
from agno.models.anthropic import Claude
from agno.models.openai.responses import OpenAIResponses


import typer
app = typer.Typer()

@tool(stop_after_tool_call=False)
def get_game_report(
    date: str, 
    home_team: str) -> str:
    """Get the report for a given game."""
    
    url=f"https://www.basketball-reference.com/boxscores/pbp/{date}0{home_team}.html"

    cols = [2,4]

    df = pd.read_html(url)[0].droplevel(0, axis=1)

    return df.to_toon()

async def run_agent(date: str, home_team: str, away_team: str) -> None:
    
    message = f'''
        Create a report of the game between {away_team} and the home team {home_team} on {date}.
        Create this report based on the play be play statistic retrieved from the data source.
        Describe the game like a game report in the newspaper.
        Include the following information in the report:
        
        - Game summary
        - Game highlights
        - Game statistics

        Format the response using markdown and include tables where appropriate.
        '''

    try:
        agent = Agent(
            model=Claude("claude-sonnet-4-5") if llm == "ANTHROPIC" else OpenAIResponses(id="gpt-4.1"),
            tools=[get_game_report],
            description='''You are an expert in generating game report for NBA games.''',
            instructions=['''
                          Describe the game like a game report in the newspaper.
                          Extract the necessary parameter values for the tool calls from the user's message.
                          Tha args for the tool "get_game_report" are:
                            - date: The date of the game in the format YYYYMMDD
                            - home_team: The abbreviation of the home team (e.g., LAL for Los Angeles Lakers)
                          '''],
            )
    
        # Run the agent
        await agent.aprint_response(
                message, 
                markdown=True, 
                stream=True,
                             )

    except Exception as e:
        print(f"Error while running the agent: {e}")
        return
    
@app.command()
def main(
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
    '''
    Entry point for typer app command.
    '''
    asyncio.run(run_agent(date, home_team, away_team))

if __name__ == "__main__":
    app()
