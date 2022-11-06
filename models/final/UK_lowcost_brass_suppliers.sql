select 
el.p_name            AS Part_Name,
    el.p_retailprice     AS RetailPrice,
    el.s_name            AS Supplier_Name,
    el.p_mfgr            AS Part_Manufacturer,
    el.s_address         AS SuppAddr,
    el.s_phone           AS Supp_Phone,
    ps.PS_AVAILQTY      AS Num_Available 
from {{ ref('Euro_lowcost_brass_suppliers') }} el
    left outer join {{ source('tcph', 'supplier') }} s on s.s_name = el.s_name
    left outer join {{ source('tcph', 'partsupp') }} ps on el.P_PARTKEY = ps.PS_PARTKEY
    and s.S_SUPPKEY = ps.PS_SUPPKEY
where n_name = 'UNITED KINGDOM'