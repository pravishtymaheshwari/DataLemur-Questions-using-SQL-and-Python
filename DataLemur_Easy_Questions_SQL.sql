----- Easy Questions-----

-- 1. Page with no likes
SELECT p.page_id FROM pages p 
left join pages_likes pl on p.page_id = pl.page_id
where pl.page_id is null 
order by p.page_id;

-- 2. Unfinished Parts
SELECT part, assembly_step FROM parts_assembly
where finish_date is null;

-- 3. Histogram of Tweets
with cte as(
SELECT user_id, count(tweet_id) as tweet_bucket FROM tweets
where datepart(year,tweet_date) = '2022'
group by user_id)
select tweet_bucket, count(user_id) as users_num from cte
group by tweet_bucket;

-- 4. Laptop vs. Mobile Viewership
SELECT 
sum(case when device_type = 'laptop' then 1 else 0 end) as laptop_viewership,
sum(case when device_type != 'laptop' then 1 else 0 end) as mobile_viewership
FROM viewership;

-- 5. Data Science Skills
SELECT candidate_id FROM candidates
where skill in ('Python','Tableau','PostgreSQL')
group by candidate_id
having count(skill) = 3
order by candidate_id;

-- 6. Average Post Hiatus
SELECT user_id, datediff(dayofyear,min(post_date),max(post_date)) as number_of_days_between
FROM posts
where datepart(year,post_date) = '2021'
group by user_id
having count(post_id) >=2 ;

-- 7. Teams Power Users
with cte as(
SELECT sender_id, count(message_id) as total_messages, dense_rank()over(order by count(message_id) desc) as rnk
FROM messages
where datepart(year,sent_date) = '2022' and datepart(month,sent_date) = '8'
group by sender_id)
select sender_id, total_messages from cte 
where rnk<=2
order by total_messages desc;

-- 8. Duplicate Job Listings
with cte as(
SELECT company_id 
FROM job_listings
group by company_id,title,description
having count(job_id)>1)
select count(company_id) as company_count
from cte;

-- 9. Cities with completed trades
with cte as(
SELECT city, count(order_id) as trade_completed, dense_rank() over(order by count(order_id) desc) as rnk
FROM trades t
inner join users u on t.user_id = u.user_id
where status = 'Completed'
group by city)
select city, trade_completed from cte where rnk<=3
order by trade_completed desc;

-- 10. Average Review Ratings
SELECT  datepart(month,submit_date) as mnth,product_id, 
round(avg(stars),2) as rating FROM reviews
group by product_id, datepart(month, submit_date)
order by mnth,product_id;

-- 11. Final Account Balance
select account_id,
sum(case when lower(transaction_type) = 'deposit' then amount else -amount end) as balance
from transactions
group by account_id;

-- 12. QuickBooks Vs TurboTax
select 
sum(case when lower(product) like '%turbotax%' then 1 else 0 end) as turbotax,
sum(case when lower(product) like '%quickbooks%' then 1 else 0 end) as quickbooks
from filed_taxes;

-- 13. App Click-through Rate(CTR)
select app_id, 
round((100.0* sum(case when event_type = 'click' then 1 else 0 end) / 
(sum(case when event_type = 'impression' then 1 else 0 end))),2) 
from events
where datepart(year, timestamp) = '2022'
group by app_id;

-- 14. Second Day Confirmation
SELECT user_id 
FROM emails em
inner join texts  tx on em.email_id = tx.email_id
where (signup_action = 'Confirmed') and (datediff(day,signup_date,action_date) = 1)


-- 15. Cards Issued Difference
SELECT card_name, (max(issued_amount)-min(issued_amount)) as amount_difference
FROM monthly_cards_issued
group by card_name
order by amount_difference desc;

-- 16. Compressed Mean
select round((1.0*sum(item_count * order_occurrences) /sum(order_occurrences)),1) as mean
from items_per_order;

-- 17. Pharmacy Analytics (Part 1)
with cte as(
select drug, sum(total_sales) - sum(cogs) as total_profit,
dense_rank()over(order by sum(total_sales) - sum(cogs) desc) as rnk 
from pharmacy_sales
group by drug)
select drug, total_profit from cte where rnk<=3;

-- 18. Pharmacy Analytics (Part 2)
with cte as(
select manufacturer, drug, sum(cogs) - sum(total_sales) as loss
from pharmacy_sales
group by manufacturer,drug)
select manufacturer, count(drug) as number_of_drugs, sum(loss) as total_loss 
from cte 
where loss > 0
group by manufacturer
order by total_loss desc;

-- 19. Pharmacy Analytics (Part 3)
select manufacturer,concat('$',round(sum(total_sales)/1000000,0),' million') as sale
from pharmacy_sales
group by manufacturer
order by sum(total_sales) desc, manufacturer;

-- 20. Most Expensive Purchase
select customer_id, max(purchase_amount) as most_expensive_purchase
from mep_transactions
group by customer_id
order by most_expensive_purchase desc;

 -- 21. ApplePay Volume
 select merchant_id, sum(case when lower(payment_method) = 'apple pay' then transaction_amount 
 else 0 end) as total_transaction_amount
 from apv_transactions
 group by merchant_id
 order by total_transaction_amount desc;

 -- 22. Subject Matter Experts
 select employee_id
 from employee_expertise
 group by employee_id
 having (count(distinct domain) =2 and sum(years_of_experience) >=12) 
 or (count(distinct domain) =1 and sum(years_of_experience) >=8);

 --23. LinkedIn Power Creators
 select profile_id from personal_profiles pp
 inner join company_pages cp on pp.employer_id = cp.company_id
 where pp.followers > cp.followers
 order by profile_id;

 --24. Highest Number of Products
 with cte as (
select 
user_id, count(product_id) as total_products,
dense_rank() over(order by count(product_id) desc, sum(spend) desc) as rnk
from user_transactions
group by user_id
having  sum(spend) >= 1000)
select user_id, total_products from cte where rnk <=3;

--25. Spare Server Capacity
select dc.datacenter_id,
monthly_capacity - sum(monthly_demand) as spare_data
from datacenters dc 
inner join forecasted_demand fd on dc.datacenter_id = fd.datacenter_id
group by dc.datacenter_id,monthly_capacity
order by dc.datacenter_id , spare_data;

--26. Top Rated Businesses
select count(business_id) as business_count,
(round(100.0*count(business_id)/(select count(distinct business_id) from reviews),0)) as percent_top_related
from trb_reviews
where review_stars in ('4','5');

--27. Ad Campaign ROAS
select advertiser_id, round(1.0*sum(revenue)/sum(spend),2) as ROAS
from ad_campaigns
group by advertiser_id
order by advertiser_id;

--28. Trade In Payouts
select store_id,sum(payout_amount) as total_payout 
from tip_trade_in_transactions as tt
inner join tip_trade_in_payouts as tp on tt.model_id = tp.model_id
group by store_id
order by total_payout desc;

--29. Webinar Popularity
select round((100.0*sum(case when lower(event_type)='webinar' then 1 else 0 end))/count(event_id),0) as prct
from marketing_touches
where datepart(month,event_date) = '4' and datepart(year, event_date) = '2022';

-- 30. Who made Quota?
select d.employee_id, 
case when sum(deal_size)>= quota then 'yes' else 'no' end as hit_or_miss
from wmq_deals as d
inner join wmq_sales_quotas as sq on d.employee_id = sq.employee_id
group by d.employee_id,quota
order by d.employee_id;



