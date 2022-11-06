select p_name, p_size, p_retailprice, s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
from {{ source('tcph', 'part') }} p 
    inner join {{ source('tcph', 'partsupp') }} ps on p.p_partkey = ps.ps_partkey
    inner join {{ source('tcph', 'supplier') }} s on ps.ps_suppkey = s.s_suppkey
    inner join {{ source('tcph', 'nation') }} n on n.n_nationKey = s.s_nationkey
    inner join {{ source('tcph', 'region') }} r on r.r_regionkey = n.n_regionKey
where 
    p.p_size = 15 
    and r.r_name = 'Europe'
    and p.p_type = '%Brass'
    and ps.ps_supplyCost = (select min(min_supply_cost.min_supply_cost)  
                                from {{ ref('min_supply_cost') }} where partkey = p.p_partkey)  
order by s_acctbal DESC, n_name, s_name, p_partkey  