'''
This is an example to demonstrate the game report tool with Agno agent.
'''
import os
os.environ["NO_PROXY"] = "localhost, 127.0.0.1"
os.environ["no_proxy"] = "localhost, 127.0.0.1"

import asyncio
from dotenv import load_dotenv
load_dotenv()

import sys
# Add src to path to import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd

from agno.agent import Agent
from agno.tools import tool
from agno.models.anthropic import Claude

import typer
app = typer.Typer()

@tool(stop_after_tool_call=False)
def get_game_report():
    """Get the report for a given game."""
    
    url="https://www.basketball-reference.com/boxscores/pbp/202511060PHO.html"

    df = pd.read_html(url)[0].droplevel(0, axis=1)

    return df[['Time','Phoenix', 'Score', 'LA Clippers']].to_markdown()

async def run_agent() -> None:
    
    message = f'''
        Create a game report based on the play be play statstic retrieved from the data source.
        Describe the game like a game report in the newspaper.
        Include the following information in the report:
        
        - Game summary
        - Game highlights
        - Game statistics

        Format the response using markdown and include tables where appropriate.
        '''

    try:
        agent = Agent(
            model=Claude("claude-sonnet-4-5"),
            tools=[get_game_report],
            description='''You are an expert in generating game report for NBA games.''',
            instructions=['''
                          Describe the game like a game report in the newspaper.
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
    ):
    '''
    Entry point for typer app command.
    '''
    asyncio.run(run_agent())

if __name__ == "__main__":
    app()