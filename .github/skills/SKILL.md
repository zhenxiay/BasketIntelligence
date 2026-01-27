---
name: Fetch data
description: Toolkit for AI agent to fetch relevant statistcs with the functions availiable from this library,
---

# Fetch relevant statistics

This skill enables AI agent to fetch relevant statistcs with the functions availiable from this library.

## When to Use This Skill

Use this skill when you are requested to fetch following statistics:
- Player statistic per game
- Player advanced statistics
- Team advanced stasistics

## Prerequisites

- Python installed on the system

## Usage Examples

### Example 1: Fetch player statistics per game
```python
from BasketIntelligence.create_season import CreateSeason

dataset = CreateSeason('2025')

dataset.read_stats_per_game()
```

### Example 2: Fetch team advanced statistics per game
```python
from BasketIntelligence.create_season import CreateSeason

dataset = CreateSeason('2025')

dataset.read_team_adv_stats()
```

## Guidelines

1. **Verify parameter values** - ALWAYS varify whether all necessary parameter values for the data fetch functions are given in user's request. If not, ask user for input values.
2. **DO NOT make up data** - If the requested data is not availiable with the functions from this library. Return 'Data not availiable with this library's functions'. DO NOT make up any data for the response!