select customer_id, upper(name) as name, email, phone, batchid from (
SELECT a.*, dense_rank() over (  partition by customer_id  ORDER	BY batchid DESC) rnk
from [dbo].[customers_raw] a ) test where rnk=1