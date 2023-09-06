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