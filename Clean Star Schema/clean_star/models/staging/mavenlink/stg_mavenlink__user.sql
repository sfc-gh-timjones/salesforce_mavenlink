select
    id as ml_user_id,
    full_name,
    email_address as email,
    headline as job_title,
    role_id
from {{ source('mavenlink', 'user') }}
where not _fivetran_deleted
