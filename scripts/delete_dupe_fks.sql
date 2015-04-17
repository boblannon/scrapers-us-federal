select 
    * 
from
    opencivicdata_organizationname 
where 
    id in (
        select 
            t.id
        from
            opencivicdata_organizationname t 
                inner join 
            (
                select 
                    oon.organization_id,
                    max(oon.start_date) start_date
                from
                    opencivicdata_organizationname oon
                        inner join 
                    (
                        select 
                            organization_id
                        from (
                                select
                                    name,
                                    organization_id,
                                    count(*) as ct
                                from 
                                    opencivicdata_organizationname
                                group by 
                                    name,
                                    organization_id
                            ) x 
                        where 
                            ct > 1
                    ) dupes using(organization_id)
                group by 
                    oon.organization_id
            ) td using (organization_id, start_date));
