with cte_arrivals as (
	select f.*
	from flightdata as f
	where f."flightDataType" = 'arrival'
),
cte_departures as (
	select f.*
	from flightdata as f
	where f."flightDataType" = 'departure'
)
select distinct de.*, ar.hashed as hashed_arrival
from cte_departures as de
left outer join cte_arrivals as ar
	on de."icao24" = ar."icao24"
	and de."firstSeen" = ar."firstSeen"
	and de."estDepartureAirport" = ar."estDepartureAirport"
	and de."lastSeen" = ar."lastSeen"
	and de."estArrivalAirport" = ar."estArrivalAirport"
where ar.hashed is not null	
