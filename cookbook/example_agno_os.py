'''
This is an example to demonstrate the functionality of the MCP server with Agno agent.
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

from agno.agent import Agent
from agno.tools import tool
from agno.models.anthropic import Claude
from agno.models.openai.responses import OpenAIResponses

from BasketIntelligence.create_season import CreateSeason

import typer
app = typer.Typer()

@tool(stop_after_tool_call=False)
def get_team_shooting_stats(
        season: str
        ):
    """Get the team shooting statistic for a given season."""
    
    return CreateSeason(season).read_team_shooting()

async def run_agent(season: str) -> None:
    '''
    Run the basketball analyst agent with the given message.
    The agent is connected with the tools from the library BasketIntelligence.
    '''
    
    message = f'''
        Provide a summary of the teams shooting performance of the NBA season {season}.

        Please include the following aspects in your analysis:
            - Describe the major differences regarding the shooting statistics among the teams, 
            - Cluster the teams based on their shooting performance into 5 distinct groups, 
            - Give each cluster a meaningful name based on their shooting characteristics,

        Format the response using markdown and include tables where appropriate.
        '''

    try:
        agent = Agent(
            model=Claude("claude-sonnet-4-5") if llm == "ANTHROPIC" else OpenAIResponses(id="gpt-4.1"),
            tools=[get_team_shooting_stats],
            description='''You are an expert in analyzing advanced statistics of NBA players and teams.,''',
            instructions=['''Extract the relevant statistics and provide insights and analysis based on the user's questions.'''],
            )
    
        # Run the agent
        await agent.aprint_response(message, markdown=True, stream=True)

    except Exception as e:
        print(f"Error while running the agent: {e}")
        return

@app.command()
def main(
    season: str = typer.Option(
        "2025", 
        help="NBA season that is to be analyzed."
        )
    ):
    '''
    Entry point for typer app command.
    '''
    asyncio.run(run_agent(season))

if __name__ == "__main__":
    app()