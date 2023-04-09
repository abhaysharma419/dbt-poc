select *
from {{ 
    metrics.calculate(
        metric('distinct_npi_numbers'),
        grain='day',
        dimensions=['email_subject']
    )
}}