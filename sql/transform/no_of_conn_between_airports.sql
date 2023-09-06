select f."estDepartureAirport", f."estArrivalAirport", count(1) as "noOfConnections"
from flightdata as f
where f."flightDataType" = 'arrival'
	and f."estDepartureAirport" is not null
	and f."estArrivalAirport" is not null
group by f."estDepartureAirport", f."estArrivalAirport"	
having count(1) > 5
order by "noOfConnections" desc
