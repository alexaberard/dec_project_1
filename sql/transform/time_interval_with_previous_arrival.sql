with cte_base as (
	select f.hashed, f."estDepartureAirport", f."estArrivalAirport", f."lastSeen"
	from flightdata as f
	where f."flightDataType" = 'arrival'
		and f."estDepartureAirport" is not null
		and f."estArrivalAirport" is not null
)
select b.*,
	b."lastSeen" - b."previousArrival" as "timeInterval"
from (
	select b.* ,
	lag(b."lastSeen") over (partition by b."estArrivalAirport" order by b."lastSeen" asc) as "previousArrival"
	from cte_base as b			 
) as b