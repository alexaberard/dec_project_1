
## DEC - project 1
## OpenSky Network API



This project consisting of implementation ETL process for getting live airflights data.

“The API lets you retrieve live airspace information for research and non-commerical purposes.”

| Author          | GitHub profile |
| ---           | --- |
|Pawel Dymek    | [pdymek](https://github.com/pdymek)|
|Cristian Ivanoff| [cristianivanoff](https://github.com/cristianivanoff) |
|Alexa Berard    | [alexaberard](https://github.com/alexaberard)|



The solutions is done with help of:
- OpenSky API documentation - https://openskynetwork.github.io/opensky-api/rest.html
- Resources provided during DEC courses - https://dataengineercamp.com/

---
### ETL
![etl_graph](/doc/imgs/ETL_diagram.JPG)
---
#### Extract


---
#### Transform
Transormation is 

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