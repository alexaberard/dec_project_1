
## DEC - project 1
## OpenSky Network API


![opensky_title](/doc/imgs/opensky_title.JPG)


This project consisting of implementation ETL process for getting live airflights data.

“The API lets you retrieve live airspace information for research and non-commerical purposes.”

---

| Author          | GitHub profile |
| ---           | --- |
|Pawel Dymek    | [pdymek](https://github.com/pdymek)|
|Cristian Ivanoff| [cristianivanoff](https://github.com/cristianivanoff) |
|Alexa Berard    | [alexaberard](https://github.com/alexaberard)|



The solutions is done with help of:
- OpenSky API documentation - https://openskynetwork.github.io/opensky-api/rest.html
- Resources provided during DEC courses - https://dataengineercamp.com/


### API Limitations

Limitations 
30 days of data.
Only 7 days of data per call.


---
### Business Questions
Objective was to extract flight data to answer these questions
- Average amount of flights by months.
- Flight time delay - estimated time vs arrival time
- Flight time statistics - max, min, spread within routes
- Route statistics - how often they are used
- Time intervals between preceeding flights on same route

---
### Architecture
![etl_graph](/doc/imgs/ETL_diagram.JPG)
---
#### Extraction

- Two airports: Krakow and Warsaw
- If  database is empty set first date to current date - 30 days
	- else: set first date to latest extraction date - select max(extractionDate) from flightdata
- Create a loop to extract 7 days per call
	- Loop and extract
- Remember that timestamps are stored in UTC



---
#### Transform

Sample sql query from `etl/sql/transform/`

``` sql
select f.*, 
	min(f."timeInterval") over (partition by f."estDepartureAirport", f."estArrivalAirport") as "minIntervalByRoute",
	max(f."timeInterval") over (partition by f."estDepartureAirport", f."estArrivalAirport") as "maxIntervalByRoute"
from (
	select f.*
		, f."lastSeen" - f."firstSeen" as "timeInterval"
	from flightdata as f
	where f."flightDataType" = 'arrival'
		and f."estDepartureAirport" is not null
		and f."estArrivalAirport" is not null
) f
```

Output tables as precalculated reports from SQL jinja templates.

![output_tables](/doc/imgs/output_tables.JPG)
---
### Project run instruction

For running ETL process the `cd` should be set as `DEC_PROJECT_1`. 

The `.env` file should be located in main folder, with listed paramters:

```
USERNAME_API_OPENSKY=""
PASSWORD_API_OPENSKY=""
SERVER_NAME=""
DATABASE_NAME=""
DATABASE_DWH_NAME=""
DB_USERNAME="postgres"
DB_PASSWORD=""      
```

The unit test function is :
> pytest etl_test

Output:
![sample_test_output](/doc/imgs/sample_test_output.JPG)


ETL execution:
>python -m etl.pipelines.opensky

Output:


![sample_etl_run](/doc/imgs/sample_etl_run.JPG)



