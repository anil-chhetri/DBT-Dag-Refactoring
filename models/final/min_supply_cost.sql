select min(ps.ps_supplycost) as min_supply_cost
    , ps.PS_PARTKEY as partKey  
from 
    {{ source('tcph', 'partsupp') }} ps
    inner join {{ source('tcph', 'supplier') }} s on ps.PS_SUPPKEY = s.s_suppkey
    inner join {{ source('tcph', 'nation') }} n on n.n_nationKey = s.s_nationkey
    inner join {{ source('tcph', 'region') }} r on n.n_regionKey = r.r_regionkey
where r.r_name = 'EUROPE'
group by ps.PS_PARTKEY