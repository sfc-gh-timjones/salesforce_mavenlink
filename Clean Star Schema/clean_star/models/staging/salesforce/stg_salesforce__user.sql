select
    id as sf_user_id,
    name as full_name,
    first_name,
    last_name,
    email,
    title,
    department,
    is_active,
    user_role_id,
    manager_id,
    created_date
from {{ source('salesforce', 'user') }}
