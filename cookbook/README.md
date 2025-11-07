# Cookbook with Agno OS

Welcome to the cookbook section! 
Here you will find examples how this library can interact with an agentic AI app like **Agno OS**.

## Setup

### Create and activate a virtual environment

```shell
uv venv cookbook

# bash
source cookbook/bin/activate

#powershell
.venv/scripts/activate

```

### Install libraries

```shell
uv pip install -U anthropic openai agno
```

### Configure LLM for the agent (OpenAI or ANTHROPIC)

Run the following command to create a .env file.

Add your ANTHROPIC_API_KEY or OPENAI_API_KEY to the file.

```bash
cp template.env .env
# Edit .env with your Open AI API key
```

## Run a cookbook

Execute the following command to see the outcome.

Make sure that you are executing the command from the project root folder.

You can add the stock of your choice with the flag --season.

#### Example of usage

```shell
uv run cookbook/example_agno_os.py --season '2024'
```

## Demo

![alt text](demo_img/image_1.png)


![alt text](demo_img/image_2.png)


![alt text](demo_img/image_3.png)


![alt text](demo_img/image_4.png)


![alt text](demo_img/image_5.png)