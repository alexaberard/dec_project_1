
with cte_base as (
	select f."estDepartureAirport", f."estArrivalAirport", f."lastSeen", to_char(f."lastSeen", 'YYYY-MM') AS "yearMonth", to_char(f."lastSeen", 'MM') as "month"
	from flightdata as f
	where f."flightDataType" = 'arrival'
		and f."estDepartureAirport" is not null
		and f."estArrivalAirport" is not null
),
cte_count_ym as (
	select ym."yearMonth", ym."month", count(1) as "noFlights"
	from cte_base as ym
	group by ym."yearMonth", ym."month"
), 
cte_monthly_avg as (
	select ym."month", avg(ym."noFlights") as "avgFlightsAmount"
	from cte_count_ym as ym
	group by ym."month"
)

select * from cte_monthly_avg order by "month" asc

