select
    id as workspace_id,
    title as project_name,
    description as project_description,
    status_key as status_code,
    status_message,
    case 
        when archived = true then 'archived'
        when status_key = 0 then 'not started'
        when status_key = 1 then 'active'
        when status_key = 2 then 'on hold'
        when status_key = 3 then 'completed'
        when status_key = 4 then 'cancelled'
        else 'unknown'
    end as project_status,
    start_date,
    due_date,
    effective_due_date,
    percentage_complete,
    budgeted as is_budgeted,
    price_in_cents / 100.0 as budget_dollars,
    budget_used_in_cents / 100.0 as budget_used_dollars,
    over_budget as is_over_budget,
    currency,
    creator_id,
    archived as is_archived,
    custom_opportunity_id_from_salesforce_ as linked_opportunity_id,
    custom_intacct_customer_id as intacct_customer_id,
    custom_sales_rep_name as sales_rep_name,
    custom_mrr_amount_in_cents / 100.0 as mrr_amount,
    custom_ps_amount_amount_in_cents / 100.0 as ps_amount,
    custom_close_won_date as deal_close_date,
    created_at as created_date
from {{ source('mavenlink', 'workspace') }}
where not _fivetran_deleted
