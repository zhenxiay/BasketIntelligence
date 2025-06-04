# 📋 BasketIntelligence

Enables analysis and dashboarding of NBA statistic with data from basket_reference.com.

Machine Learning functionality for decomposition of advanced statistics (BPM, PER etc.) will be provided.

Data ingestion methods to to Google BigQuery, Fabric Lakehouse and PostgresSQL availiable.

## 📋 Link to an example dashboard (looker studio):
https://lookerstudio.google.com/reporting/10cd2c10-17f3-4e0e-aa9c-01fb6470516e/page/x05ZE

## 🚀 Getting Started

Clone the repository:

```bash
git clone https://github.com/zhenxiay/StockIntelligence.git
cd StockIntelligence
```

## 📦 Installation Options

You can install this libary either with pip or uv. Choose the option that best suits your needs.

### Option 1: Install with pip install

Install using pip install:

```bash
pip install https://github.com/zhenxiay/BasketIntelligence.git
```

### Option 2: Install with uv

#### Create a new directory for our project

⚙️ To add this libary to an existing uv project, pls skip the first 2 steps

```bash
uv init StockIntelligence
cd StockIntelligence
```

#### Create virtual environment and activate it

```bash
uv venv
source .venv/bin/activate
```

#### Install dependencies
```bash
uv add https://github.com/zhenxiay/BasketIntelligence.git

## How to import and use library

```python
from BasketIntelligence.create_season import CreateSeason
```
## 🚀 Getting Started

### Add year as an argument to retrieve the data from a season

```python
dataset = CreateSeason("2025")
```

### Use method to read per game data or adv stats

```python
dataset.read_stats_per_game()

dataset.read_adv_stats()

dataset.read_team_adv_stats()
```

 ## Method to load data
 
 Currently the libaray offers API to load data to postgresSQL, Google big query or to MS Fabric lakehouse:
 
 #### Create a dataset that is to be loaded with pollowing parameters:
 
 year, big query project id, dataset id and table id

 ```python
 dataset = LoadSeasonData("2025","keen-vial-420113","BasketIntelligence")
```
 
 #### Load to big query:
 
 
```python
 dataset.load_per_game_to_big_query("per_game_stats")
```
 
 #### Load to MS fabric lakehouse:
 
 ```python
 datasetload_adv_stats_to_lakehouse()
 ```
