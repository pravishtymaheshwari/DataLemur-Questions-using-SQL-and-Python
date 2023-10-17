-- MEDIUM QUESTIONS

--31. User's Thirs Transaction
with cte as(
select user_id, spend, transaction_date, dense_rank()over(partition by user_id order by transaction_date) as rnk
from m_transactions)
select user_id, spend, transaction_date from cte where rnk=3;

--32. Sending Vs Opening Snapchat
with cte as(
select age_bucket,
sum(case when activity_type = 'send' then time_spent end) as send_time,
sum(case when activity_type = 'open' then time_spent end) as open_time
from m_so_activities as ac
inner join m_so_age_breakdown ab on ac.user_id = ab.user_id
group by age_bucket)
select age_bucket, round(100.0*send_time/(send_time + open_time),2) as send_prct,
round(100.0*open_time / (send_time + open_time),2) as open_prct
from cte;

--33. Tweets'Rolling Averages
select user_id,tweet_date,
round((avg(1.0*tweet_count)over(partition by user_id order by tweet_date rows between 2 preceding and current row)),2) as ld
from m_tra_tweets;

--34. Highest Grossing Items
select * from m_product_spend;
with cte as(
select category,product,sum(spend) as total_spend,
dense_rank()over(partition by category order by sum(spend) desc) as rnk
from m_product_spend
where datepart(year,transaction_date) = '2022'
group by category, product)
select category, product, total_spend from cte
where rnk <=2

--35. Top 5 Artists
with cte as(
select a.artist_name,count(gr.song_id) as song_count,
dense_rank() over(order by count(gr.song_id) desc) as rnk 
from m_t_a_artists a
inner join m_t_a_songs s on a.artist_id = s.artist_id
inner join m_t_a_global_song_rank as gr on s.song_id = gr.song_id
where rank <=10
group by a.artist_name)
select artist_name,rnk from cte where rnk<=5;

-- 36.Signup Activation Rate
SELECT round(1.0*count(case when t.signup_action = 'Confirmed' then e.email_id end) / 
count(distinct e.email_id),2)
from m_sar_emails e
left join m_sar_texts t on e.email_id = t.email_id;


-- 38. Spotify Streaming History (Good Question)
with cte as(
select user_id,song_id, count(cast(listen_time as date)) as song_plays from m_ssh_songs_weekly
where cast(listen_time as date) <= '08/04/2022'
group by user_id, song_id
UNION ALL
select user_id, song_id, song_plays from m_ssh_songs_history)
select user_id, song_id, sum(song_plays) as cumulative_count from cte
group by user_id, song_id
order by cumulative_count desc;



--40. Pharmacy Analytics(Part-4)
with cte as(
select manufacturer,drug,
dense_rank()over(partition by manufacturer order by sum(units_sold) desc) as rnk 
from pharmacy_sales
group by manufacturer,drug)
select manufacturer, drug from cte where rnk<=2;

create table product_transactions(transaction_id int,product_id int,user_id int, transaction_date datetime);

insert into product_transactions values
(231574,    111,    234,    '03/01/2022 12:00:00'),
(231574,    444,	234,	'03/01/2022 12:00:00'),
(231574,	222,	234,	'03/01/2022 12:00:00'),
(137124,	444,	125,	'03/05/2022 12:00:00'),
(523152,	222,	746,	'03/06/2022 12:00:00'),
(141415,	333,	235,	'03/02/2022 12:00:00'),
(523152,	444,	746,	'03/06/2022 12:00:00'),
(137124,	111,	125,	'03/05/2022 12:00:00'),
(256234,	444,	311,	'03/07/2022 12:00:00'),
(256234,	111,	311,	'03/07/2022 12:00:00');


--41. Frequently Purchased Pairs (Check Ankit Bansal Notes)
with cte as(
SELECT transaction_date, STRING_AGG(CAST(product_id AS VARCHAR), ',') WITHIN GROUP (ORDER BY product_id) as result from product_transactions
GROUP BY transaction_date
having count(distinct product_id) > 1)
select distinct result from cte
order by result;


--42. Supercloud Customer
SELECT customer_id FROM m_sc_customer_contracts c
inner join m_sc_products p on c.product_id = p.product_id
group by customer_id
having count(distinct product_category) = (select count(distinct product_category) from m_sc_products);


--43. Odd and Even Measurements
with cte as(
SELECT cast(measurement_time as date) as measurement_day, measurement_value,
dense_rank() over(partition by cast(measurement_time as date) order by measurement_time asc) as rnk 
FROM m_oem_measurements)
, cte1 as (select measurement_day,
sum(case when rnk %2 != 0 then measurement_value end) as odd_sum,
sum(case when rnk %2 = 0 then measurement_value end) as even_sum
from cte
group by measurement_day)
select measurement_day, sum(odd_sum) as odd_sum,sum(even_sum) as even_sum
from cte1
group by measurement_day;


-- 44. Booking Referral Source
with cte as(
select user_id, booking_date, channel, dense_rank()over(partition by user_id order by b.booking_id) as rnk
from m_brs_bookings as b
inner join m_brs_booking_attribution as ba on b.booking_id = ba.booking_id)
, cte1 as (
select channel, count(booking_date) as total_bookings from cte 
where rnk = 1
group by channel)
select top 1 round(100.0*total_bookings / (select sum(total_bookings) from cte1) ,2 )from cte1
where channel is not null;


--45. Shopping Spree
with cte as(
select user_id, transaction_date,
lead(transaction_date,1)over(partition by user_id order by transaction_date) as lead1,
lead(transaction_date,2)over(partition by user_id order by transaction_date) as lead2
from m_uss_transactions
transactions)
select user_id from cte 
where lead1-transaction_date= 1 and lead2-lead1 = 1
order by user_id;


--46. 2nd Ride Delay
with cte as(
select u.user_id, ride_id, registration_date, ride_date,
row_number() over(partition by u.user_id order by ride_date) as rn from m_rd_users u
inner join m_rd_rides r on u.user_id = r.user_id)
,cte1 as (select * from cte where rn = 1 and registration_date = ride_date)
select round(1.0*sum(cte.ride_date - cte.registration_date) / count(cte.ride_id) ,2) as ride_delay
from cte inner join cte1 on cte.user_id = cte1.user_id
where cte.rn =2;


--47. Histogram of Users and Purchases
with cte as(
select * ,dense_rank()over(partition by user_id order by transaction_date desc) as rnk
from m_hup_user_transactions)
select transaction_date, user_id, count(product_id) as cnt
from cte
where rnk = 1
group by user_id, transaction_date
order by transaction_date;


--48. Google Maps Flagged UGC
with cte as(
select place_category,count(content_tag) as cnt,
dense_rank()over(order by count(content_tag) desc) as rnk
from m_gmf_place_info as pl inner join m_gmf_maps_ugc_review as mu on pl.place_id = mu.place_id
where lower(content_tag) = 'off-topic'
group by place_category)
select place_category from cte 
where rnk=1 
order by place_category;


--49. Compressed Mode
with cte as(
select item_count,dense_rank()over(order by sum(order_occurrences) desc) as occurence_count from items_per_order
group by item_count
)
select item_count from cte where occurence_count = 1
order by item_count;


--50. Card Launch Success
with cte as(
SELECT card_name, sum(issued_amount) as card_count, concat(issue_year,'-',issue_month) as issue_date,
dense_rank()over(partition by card_name order by concat(issue_year,'-',issue_month)) 
as rnk 
FROM monthly_cards_issued
group by card_name,concat(issue_year,'-',issue_month))
select card_name, card_count from cte 
where rnk=1
order by card_count desc;


insert into phone_calls values
(24,36,'07/31/2022 07:16:09')
insert into phone_calls values
(5,33,'05/14/2022 19:33:04'),
(11,31,'07/11/2022 04:18:03'),
(13,6,'07/16/2022 01:45:59'),
(5,34,'08/07/2022 18:50:13'),
(45,35,'06/14/2022 00:42:39'),
(12,50,'08/04/2022 02:01:16'),
(14,31,'10/29/2022 00:44:04'),
(7,4,'09/03/2022 00:04:30'),
(37,46,'09/01/2022 09:50:24'),
(35,36,'11/01/2022 11:00:00');


--51. International Call Percentage
SELECT round(100.0*count(pc.caller_id)/(select count(caller_id) from phone_calls),1)
FROM m_icp_phone_info pi_caller inner join
phone_calls pc on pc.caller_id = pi_caller.caller_id
inner join m_icp_phone_info pi_receiver on pc.receiver_id = pi_receiver.caller_id 
where pi_caller.country_id != pi_receiver.country_id;


--52. LinkedIn Power Creators(Part 2)
with cte as(
select pp.profile_id as creator_id, pp.followers as creator_followers,
cp.followers as company_followers,
dense_rank()over(PARTITION BY pp.profile_id order by cp.followers desc) as rnk
from m_lpc_personal_profiles as pp
inner join m_lpc_employee_company emp on pp.profile_id = emp.personal_profile_id
inner join m_lpc_company_pages cp on emp.company_id = cp.company_id)
select distinct creator_id from cte
where creator_followers > company_followers and rnk =1
order by creator_id;


--53. Unique Money Transfer Relationships 
with cte as(
select distinct p.payer_id,p.recipient_id from m_umtp_payments p 
inner join m_umtp_payments p1 on p.payer_id = p1.recipient_id and p.recipient_id = p1.payer_id)
select count(*)/2 from cte;


--54. User Session Activity
select user_id, session_type,
dense_rank()over(partition by session_type order by sum(duration) desc) as dnk
from m_usa_sessions
where start_date > '2022-01-01' and start_date <'2022-02-01'
group by user_id, session_type;


--55. First Transaction
with cte as (
select user_id, sum(spend) as total_spend, 
dense_rank()over(partition by user_id order by transaction_date) as dnk
from m_ft_user_transactions
group by user_id,transaction_date)
select count(user_id) as total_users from cte
where dnk =1 and total_spend >=50;


--56. Email Table Transformation
select user_id,
max(case when email_type = 'personal' then email end) as personal_email,
max(case when email_type = 'business' then email end) as business_email,
max(case when email_type = 'recovery' then email end) as recovery_email
from m_ett_users
group by user_id
order by user_id;


--57. Photoshop Revenue Analysis
with cte as (
select customer_id from m_pra_adobe_transactions
where lower(product) = 'photoshop')
select adt.customer_id, sum(revenue) as total_spent from m_pra_adobe_transactions adt
inner join cte on adt.customer_id = cte.customer_id
where lower(adt.product) != 'photoshop'
group by adt.customer_id
order by adt.customer_id;


--58. Cumulative Purchases by Product Type
select order_date, product_type, sum(quantity)over(partition by product_type order by order_date) as total_quantities
from m_cppt_total_trans
order by order_date;


-- 59. Invalid Search Results
with cte as(
select country, sum(num_search) as total_search, sum(num_search*invalid_result_pct*0.01) as total_invalid_search
from m_isr_search_category
where invalid_result_pct is not null
group by country)
select country, total_search,round(100.0*total_invalid_search / total_search,2) as overal_prct from cte
order by country;


--60. Repeat Purchases on Multiple Days
with cte as(
select user_id, product_id, count(distinct cast(purchase_date as date)) as repeated_orders from m_rpmd_purchases
group by user_id, product_id)
select count(distinct user_id) as repeated_customers from cte
where repeated_orders > 1;


--61. Compensation Outliers
with cte as(
select employee_id, salary, avg(salary)over(partition by title) as average
from m_co_employee_pay)
select employee_id, salary, case when salary > 2*average then 'Overpaid' ELSE
(case when salary < 0.5*average then 'Underpaid' end) end as decision
from cte
where (salary > 2*average) or (salary < 0.5*average);


--62. Y-on-Y Growth Rate
with cte as(
select datepart(year, transaction_date) as yr, product_id, sum(spend) as current_year_spend
from h_ygr_user_transactions
group by datepart(year, transaction_date), product_id)
,cte1 as (select *, lag(current_year_spend)over(partition by product_id order by yr) as previous_year_spend from cte)
select yr, product_id, current_year_spend, previous_year_spend, 
round((100.0*(current_year_spend - previous_year_spend) / previous_year_spend),2) as yoy from cte1;


--63. Consecutive Filing Years
with cte as(
select user_id, 
lead(product,1) over(partition by user_id order by datepart(year,filing_date)) as next_year_product,
lead(product,2) over(partition by user_id order by datepart(year,filing_date)) as next_to_next_year_product
from filed_taxes
where lower(product) like '%turbotax%')
select distinct user_id from cte
where next_year_product is not null and next_to_next_year_product is not null
order by user_id;