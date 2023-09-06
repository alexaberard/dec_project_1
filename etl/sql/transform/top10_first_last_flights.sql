with cte_arrivals_first as (
	select f.*, 'frst'as "type"
	from flightdata as f
	where f."flightDataType" = 'arrival'
	order by f."lastSeen" asc
	limit 10
),
cte_arrivals_last as (
	select f.*, 'last' as "type"
	from flightdata as f
	where f."flightDataType" = 'arrival'
	order by f."lastSeen" desc
	limit 10
)
select * from cte_arrivals_first
union all
select * from cte_arrivals_last