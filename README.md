# Project plan 

## Objective 
The object of our project is to provide analytical datasets from the OpenSky API.

## Consumers 
The users of our datasets are Data Analysts and developers of the OpenSky Network application (https://opensky-network.org/).

## Questions 
We're attempting to answer the following questions:

> - What are the most frequented flight routes within Poland?
> - What is the busiest season?
> - What are the most popular aircrafts?
> - What is the average flight time between 2 airports?
> - What time of day sees the highest number of arrivals?
> - What time of day sees the highest number of departures?
> - Has the ukraine war affected flight paths?
> - What is the daily flight count?


## Source datasets 
What datasets are you sourcing from? How frequently are the source datasets updating?

Example: 

| Source name | Source type | Source documentation |
| - | - | - |
| OpenSky API | REST API | - | 
**Add individual API names**

## Solution architecture
How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution? How do we automate the entire running of the solution? 

- What data extraction patterns are you going to be using? Full extract (function to pull from past 2 years, 30 days at a time) initially, then incremental extract 
- What data loading patterns are you going to be using? --> upsert 
- What data transformation patterns are you going to be performing?
- Aggregation function e.g. `avg`, `sum`, `max`, `min`, `count`, `rank`
- Grouping i.e. `group by`
- Window function e.g. `partition by`
- Calculation e.g. `column_A + column_B`
- Data type casting
- Filtering e.g. `where`, `having`
- Sorting
- Joins/merges
- Unions
- Renaming e.g. `df.rename(columns={"old":"new:})` or `columnA as column_A`

We recommend using a diagramming tool like [draw.io](https://draw.io/) to create your architecture diagram. 

Here is a sample solution architecture diagram: 

![images/sample-solution-architecture-diagram.png](images/sample-solution-architecture-diagram.png)

## Breakdown of tasks 
Task breakdown stored here: https://trello.com/b/3xiyNull/project-1
