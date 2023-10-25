# DataLemur Easy Questions using Python


```python
# Connection Setup
import pandas as pd
import sqlalchemy as sal
import numpy as np

Engine = sal.create_engine('mssql://HP\SQLEXPRESS/DATALEMUR_DATABASE?driver=ODBC+Driver+17+for+SQL+Server')
Conn = Engine.connect()
```


```python
#2. Tesla is investigating production bottlenecks and they need your help to extract the relevant data. Write a query to 
#determine which parts have begun the assembly process but are not yet finished.
# Assumptions:
# parts_assembly table contains all parts currently in production, each at varying stages of the assembly process.
# An unfinished part is one that lacks a finish_date.

df_parts_assembly = pd.read_sql_query('select * from parts_assembly',Conn)
df_parts_assembly[df_parts_assembly['finish_date'].isnull()][['part','assembly_step']] 

```


```python
#3. Assume you're given a table Twitter tweet data, write a query to obtain a histogram of tweets posted per user in 2022. 
# Output the tweet count per user as the bucket and the number of Twitter users who fall into that bucket.
# In other words, group the users by the number of tweets they posted in 2022 and count the number of users in each group.
df_tweets = pd.read_sql_query('select * from tweets',Conn)
df1 = df_tweets[df_tweets['tweet_date'].dt.year == 2022].groupby(['user_id'])['tweet_id'].count().reset_index(name = 'tweet_bucket')
df1.groupby('tweet_bucket')['user_id'].count().reset_index(name = 'users_num')

```


```python
#4. Assume you're given the table on user viewership categorised by device type where the three types are laptop, tablet, and phone.
# Write a query that calculates the total viewership for laptops and mobile devices where mobile is defined as the sum of 
#tablet and phone viewership. Output the total viewership for laptops as laptop_reviews and the total viewership for mobile 
#devices as mobile_views.
import numpy as np
df_viewership = pd.read_sql_query('select * from viewership',Conn)
df_viewership= df_viewerships[['user_id','device_type','view_time']]
df_viewership['category'] = np.where(df_viewership['device_type']== 'laptop', 'laptop_viewership','mobile_viewership')
df_viewership.groupby('category')['user_id'].count().reset_index(name='total_viewership')
# laptop_viewership = np.where(df_viewership['device_type']== 'laptop', 1,0).sum()
# mobile_viewership = np.where(df_viewership['device_type'] != 'laptop', 1,0).sum()
# print('laptop_viewership: {}'.format(laptop_viewership))
# print('mobile_viewership: {}'.format(mobile_viewership))


```


```python
#5. Given a table of candidates and their skills, you're tasked with finding the candidates best suited for an open Data Science
#job. You want to find candidates who are proficient in Python, Tableau, and PostgreSQL.
# Write a query to list the candidates who possess all of the required skills for the job. Sort the output by candidate ID in 
#ascending order.

df_candidates = pd.read_sql_query('select * from candidates',Conn)
df_candidates = df_candidates[df_candidates['skill'].isin(['Python','Tableau','PostgreSQL'])].groupby('candidate_id')['skill'].nunique().reset_index(name = 'cnt')
df_candidates[df_candidates['cnt']==3]['candidate_id'].sort_values()

```


```python
#6. Given a table of Facebook posts, for each user who posted at least twice in 2021, write a query to find the number of days 
#between each user’s first post of the year and last post of the year in the year 2021. Output the user and number of the days 
#between each user's first and last post.

df_posts = pd.read_sql_query('select * from posts',Conn)
df_posts = df_posts[df_posts['post_date'].dt.year == 2021].groupby('user_id').filter(lambda x: len(x)>=2)
df_posts.groupby('user_id')['post_date'].apply(lambda x: (x.max() - x.min()).days).reset_index(name = 'number_of_days_between')
```


```python
#7. Write a query to identify the top 2 Power Users who sent the highest number of messages on Microsoft Teams in August 2022. 
#Display the IDs of these 2 users along with the total number of messages they sent. Output the results in descending order 
#based on the count of the messages.
df_messages = pd.read_sql_query('select * from messages',Conn)
df_messages = df_messages[(df_messages['sent_date'].dt.month == 8 )& (df_messages['sent_date'].dt.year == 2022)].groupby('sender_id')['message_id'].count().reset_index(name = 'total_messages')
df_messages['rnk'] = df_messages['total_messages'].rank(method='dense',ascending=False)
df_messages[df_messages['rnk'] <=2][['sender_id','total_messages']].sort_values('total_messages',ascending=False)
```


```python
#8. Assume you're given a table containing job postings from various companies on the LinkedIn platform. Write a query to 
#retrieve the count of companies that have posted duplicate job listings.
#Definition:Duplicate job listings are defined as two job listings within the same company that share identical titles and 
#descriptions.

df_joblistings = pd.read_sql_query('select * from job_listings',Conn)
df_joblistings = df_joblistings.groupby(['company_id','title','description'])['job_id'].count().reset_index(name='cnt')
df_joblistings[df_joblistings['cnt']>1]['company_id'].nunique()
```


```python
#9. Assume you're given the tables containing completed trade orders and user details in a Robinhood trading system.
# Write a query to retrieve the top three cities that have the highest number of completed trade orders listed in descending 
#order. Output the city name and the corresponding number of completed trade orders.
df_trades = pd.read_sql_query('select * from trades',Conn)
df_users = pd.read_sql_query('select * from users', Conn)
df_merged = df_trades.merge(df_users, on='user_id')
df_merged = df_merged[df_merged['status']=='Completed'].groupby('city')['order_id'].count().reset_index(name='trade_completed')
df_merged['rank'] = df_merged['trade_completed'].rank(method = 'dense',ascending=False)
df_merged[df_merged['rank']<=3][['city','trade_completed']].sort_values('trade_completed',ascending=False)

```


```python
#10. Given the reviews table, write a query to retrieve the average star rating for each product, grouped by month. 
#The output should display the month as a numerical value, product ID, and average star rating rounded to two decimal places. 
#Sort the output first by month and then by product ID.
df_reviews = pd.read_sql_query('select * from reviews',Conn)

df_reviews['month'] = df_reviews['submit_date'].dt.month
df_reviews = df_reviews.groupby(['month','product_id'])['stars'].mean().reset_index(name='avg_rating').round(2).sort_values(['month','product_id'])
df_reviews
```


```python
#11. Given a table containing information about bank deposits and withdrawals made using Paypal, write a query to retrieve the 
#final account balance for each account, taking into account all the transactions recorded in the table with the assumption that
#there are no missing transactions.account.
import numpy as np
df_transactions = pd.read_sql_query('select * from transactions',Conn)

df_transactions['amount'] = np.where(df_transactions['transaction_type']=='Deposit',df_transactions['amount'],-df_transactions['amount'])
df_transactions.groupby('account_id')['amount'].sum().reset_index(name='final_balance')
```


```python
#12. Write a query to determine the total number of tax filings made using TurboTax and QuickBooks. Each user can file taxes 
#once a year using only one product.
import numpy as np
df_filed_taxes = pd.read_sql_query('select * from filed_taxes',Conn)
df_filed_taxes['product_type'] = np.where(df_filed_taxes['product'].str.contains('TurboTax',case=False),'TurboTax','QuickBooks')
df_filed_taxes.groupby('product_type')['filing_id'].count().reset_index(name = 'total_filings')
```


```python
#13. Assume you have an events table on Facebook app analytics. Write a query to calculate the click-through rate (CTR) for 
#the app in 2022 and round the results to 2 decimal places.
#Percentage of click-through rate (CTR) = 100.0 * Number of clicks / Number of impressions
#To avoid integer division, multiply the CTR by 100.0, not 100.

df_events = pd.read_sql_query('select * from events',Conn)
df_events = df_events[df_events['timestamp'].dt.year==2022]
pivot_df = df_events.pivot_table(index='app_id', columns='event_type', aggfunc='size')
pivot_df['CTR'] = (pivot_df['click'] / pivot_df['impression']) * 100.0
pivot_df['CTR'] = pivot_df['CTR'].round(2)
pivot_df
```


```python
#14. Assume you're given tables with information about TikTok user sign-ups and confirmations through email and text. New users 
#on TikTok sign up using their email addresses, and upon sign-up, each user receives a text message confirmation to activate 
#their account. Write a query to display the user IDs of those who did not confirm their sign-up on the first day, but confirmed
#on the second day.
#Definition:
#action_date refers to the date when users activated their accounts and confirmed their sign-up through text messages.

df_emails = pd.read_sql_query('select * from emails',Conn)
df_texts = pd.read_sql_query('select * from texts',Conn)
df_merged = df_emails.merge(df_texts,on='email_id')
df_merged = df_merged[((df_merged['action_date'] - df_merged['signup_date']).dt.days == 1)&(df_merged['signup_action']=='Confirmed')]['user_id']
df_merged
```


```python
#15.Your team at JPMorgan Chase is preparing to launch a new credit card, and to gain some insights, you're analyzing how many 
#credit cards were issued each month.Write a query that outputs the name of each credit card and the difference in the number 
#of issued cards between the month with the highest issuance cards and the lowest issuance. 
#Arrange the results based on the largest disparity.

df_monthly_cards_issued = pd.read_sql_query('select * from monthly_cards_issued',Conn)
df = df_monthly_cards_issued.groupby('card_name')['issued_amount'].agg(['max','min']).reset_index()
df['amount_difference'] = df['max'] - df['min']
df[['card_name','amount_difference']].sort_values('amount_difference',ascending=False)

```


```python
#16. You're trying to find the mean number of items per order on Alibaba, rounded to 1 decimal place using tables which includes
#information on the count of items in each order (item_count table) and the corresponding number of orders for each item count 
#(order_occurrences table).
df_itemsperorder = pd.read_sql_query('select * from items_per_order',Conn)
df_itemsperorder['total_orders'] = df_itemsperorder['order_occurrences']*df_itemsperorder['item_count']
(df_itemsperorder['total_orders'].sum() / df_itemsperorder['order_occurrences'].sum()).round(1)


```


```python
#17.CVS Health is trying to better understand its pharmacy sales, and how well different products are selling. 
#Each drug can only be produced by one manufacturer. Write a query to find the top 3 most profitable drugs sold, and 
#how much profit they made. Assume that there are no ties in the profits. Display the result from the highest to the 
#lowest total profit
df_pharmacysales = pd.read_sql_query('select * from pharmacy_sales',Conn)

df_pharmacysales = df_pharmacysales.groupby('drug')[['total_sales','cogs']].sum().reset_index()
df_pharmacysales['total_profit'] = df_pharmacysales['total_sales'] - df_pharmacysales['cogs']
df_pharmacysales['rank'] = df_pharmacysales['total_profit'].rank(method='dense',ascending=False)
df_pharmacysales[df_pharmacysales['rank']<=3][['drug','total_profit']].sort_values('total_profit',ascending=False)


```


```python
#18. CVS Health is analyzing its pharmacy sales data, and how well different products are selling in the market. Each drug 
#is exclusively manufactured by a single manufacturer.Write a query to identify the manufacturers associated with the drugs
#that resulted in losses for CVS Health and calculate the total amount of losses incurred.Output the manufacturer's name, 
#the number of drugs associated with losses, and the total losses in absolute value. Display the results sorted in 
#descending order with the highest losses displayed at the top.

df_pharmacysales = pd.read_sql_query('select * from pharmacy_sales',Conn)
df_pharmacysales['total_loss'] = df_pharmacysales['cogs'] - df_pharmacysales['total_sales']
df_pharmacysales = df_pharmacysales[df_pharmacysales['total_loss'] >0].groupby('manufacturer')[['drug','total_loss']].agg({'drug':'count','total_loss':'sum'}).sort_values('total_loss',ascending=False).reset_index()
df_pharmacysales

```


```python
#19. Write a query to calculate the total drug sales for each manufacturer. Round the answer to the nearest million and report 
#your results in descending order of total sales. In case of any duplicates, sort them alphabetically by the manufacturer name.
#Since this data will be displayed on a dashboard viewed by business stakeholders, please format your results as follows: 
#"$36 million".

df_pharmacysales = pd.read_sql_query('select * from pharmacy_sales',Conn)
df_pharmacysales = df_pharmacysales.groupby('manufacturer')['total_sales'].sum().reset_index(name = 'total_drug_sales')
df_pharmacysales['drug_sales'] = "$" + (df_pharmacysales['total_drug_sales']/1000000).round().astype(str) +" million"
df_pharmacysales.sort_values('total_drug_sales', ascending=False)[['manufacturer','drug_sales']]

```


```python
#20. Amazon is trying to identify their high-end customers. To do so, they first need your help to write a query that obtains 
#the most expensive purchase made by each customer. Order the results by the most expensive purchase first.

df_transactions = pd.read_sql_query('select * from mep_transactions',Conn)
df_transactions.groupby('customer_id')['purchase_amount'].max().reset_index(name = 'most_expensive_purchase').sort_values('most_expensive_purchase',ascending=False)
```


```python
#21. Visa is analysing its partnership with ApplyPay. Calculate the total transaction volume for each merchant where the 
#transaction was performed via ApplePay.Output the merchant ID and the total transactions. For merchants with no ApplePay 
#transactions, output their total transaction volume as 0. Display the result in descending order of the transaction volume.
import numpy as np
df_transactions = pd.read_sql_query('select * from apv_transactions',Conn)
df_transactions['transaction_amount'] = np.where(df_transactions['payment_method'].str.lower() == 'apple pay',df_transactions[
    'transaction_amount'],0)
df_transactions.groupby('merchant_id')['transaction_amount'].sum().reset_index(name = 'total_transaction_amount').sort_values('total_transaction_amount',ascending=False)

```


```python
#22. You are tasked with identifying Subject Matter Experts (SMEs) at Accenture based on their work experience in specific domains. An employee qualifies as an SME if they meet either of the following criteria:
#They have 8 or more years of work experience in a single domain.
#They have 12 or more years of work experience across two different domains.
#Write a query to return the employee IDs of all the subject matter experts at Accenture.
df_employeeexpertise = pd.read_sql_query('select * from employee_expertise',Conn)
result = df_employeeexpertise.groupby('employee_id').filter(lambda x: (
    (x['domain'].nunique() == 2 and x['years_of_experience'].sum() >= 12) or
    (x['domain'].nunique() == 1 and x['years_of_experience'].sum() >= 8)
   )
)
result['employee_id'].drop_duplicates()
```


```python
#23. The LinkedIn Creator team is seeking out individuals who have a strong influence on the platform, utilizing their personal 
#profiles as a company or influencer page. To identify such power creators, we can compare the number of followers on their 
#LinkedIn page with the number of followers on the company they work for. If a person's LinkedIn page has more followers 
#than their company, we consider them to be a power creator.Write a query to retrieve the profile IDs of these LinkedIn power creators ordered in ascending order based on their IDs.
df_personalprofile = pd.read_sql_query('select * from personal_profiles',Conn)
df_companypages = pd.read_sql_query('select * from company_pages',Conn)
df_merged = df_personalprofile.merge(df_companypages, left_on = 'employer_id',right_on = 'company_id')
df_merged[df_merged['followers_x']> df_merged['followers_y']]['profile_id'].sort_values()
```


```python
#24. Assume that you are given the table below containing information on various orders made by eBay customers. Write a query
#to obtain the user IDs and number of products purchased by the top 3 customers; these customers must have spent at least 
#$1,000 in total. Output the user id and number of products in descending order. To break ties (i.e., if 2 customers both 
#bought 10 products), the user who spent more should take precedence.

df_usertransactions = pd.read_sql_query('select * from user_transactions', Conn) 
df_usertransactions = df_usertransactions.groupby('user_id').agg({'product_id':'count','spend':'sum'}).reset_index()
df_usertransactions = df_usertransactions[df_usertransactions['spend']>=1000].sort_values(['product_id','spend'],ascending=[False,False])
df_usertransactions['rank'] = df_usertransactions['product_id'].rank(method='first',ascending=False)
df_usertransactions = df_usertransactions[df_usertransactions['rank'] <=3][['user_id','product_id']]
df_usertransactions
```


```python
#25. Microsoft Azure's capacity planning team wants to understand how much data its customers are using, and how much spare 
#capacity is left in each of its data centers. You’re given three tables: customers, data centers, and forecasted_demand.Write 
#a query to find each data centre’s total unused server capacity. Output the data center id in ascending order and the total 
#spare capacity.

df_datacenter = pd.read_sql_query('select * from datacenters',Conn)
df_forecasteddemand = pd.read_sql_query('select * from forecasted_demand',Conn)
df_merged = df_datacenter.merge(df_forecasteddemand,on='datacenter_id')
df_merged = df_merged.groupby(['datacenter_id','monthly_capacity'])['monthly_demand'].sum().reset_index()
df_merged['spare_data'] = df_merged['monthly_capacity'] - df_merged['monthly_demand']
df_merged[['datacenter_id','spare_data']].sort_values(['datacenter_id','spare_data'],ascending=[True,True])

```


```python
#26.Assume you are given the table below containing information on user reviews. Write a query to obtain the number and percentage 
#of businesses that are top rated. A top-rated busines is defined as one whose reviews contain only 4 or 5 stars.Output the 
#number of businesses and percentage of top rated businesses rounded to the nearest integer.Assumption:Each business has only one review

df_reviews = pd.read_sql_query('select * from trb_reviews',Conn)
total_business = df_reviews['business_id'].count()
top_rated_business = df_reviews[df_reviews['review_stars'].isin([4,5])]['business_id'].count()
percentage_of_top_rated = (100.0*top_rated_business/total_business).round()
result_df = pd.DataFrame({'top_rated_business':[top_rated_business],'percentage_of_top_rated':[percentage_of_top_rated]})
result_df  
```


```python
#27. Google marketing managers are analyzing the performance of various advertising accounts over the last month. They need your
#help to gather the relevant data. Write a query to calculate the return on ad spend (ROAS) for each advertiser across all ad 
#campaigns. Round your answer to 2 decimal places, and order your output by the advertiser_id.
#Hint: ROAS = Ad Revenue / Ad Spend

df_adcampaigns = pd.read_sql_query('select * from ad_campaigns',Conn)
df_adcampaigns = df_adcampaigns.groupby('advertiser_id').agg(total_revenue=('revenue', 'sum'), total_spend=('spend', 'sum')).reset_index()
df_adcampaigns['ROAS'] = (df_adcampaigns['total_revenue'] / df_adcampaigns['total_spend']).round(2)
df_adcampaigns[['advertiser_id','ROAS']].sort_values('advertiser_id')
```


```python
#28. Trde In Payouts
df_trade_transaction = pd.read_sql_query('select * from tip_trade_in_transactions',Conn)
df_trade_payouts = pd.read_sql_query('select * from tip_trade_in_payouts',Conn)
df_merged = df_trade_transaction.merge(df_trade_payouts,on='model_id')
df_merged.groupby('store_id')['payout_amount'].sum().reset_index(name = 'total_payout').sort_values('total_payout',ascending=False)

```


```python
#29. Webinar Popularity
#As a Data Analyst on Snowflake's Marketing Analytics team, you're analyzing the CRM to determine what percent of marketing touches were of type "webinar" in April 2022. Round your percentage to the nearest integer.
df_marketing = pd.read_sql_query('select * from marketing_touches',Conn)
df_marketing = df_marketing[(df_marketing['event_date'].dt.year == 2022) & (df_marketing['event_date'].dt.month == 4)]
perct = 100*df_marketing[df_marketing['event_type'].str.lower() == 'webinar']['event_id'].count()/ df_marketing['event_id'].count()
perct
```


```python
#30. Who made Quota?
#As a data analyst on the Oracle Sales Operations team, you are given a list of salespeople’s deals, and the annual quota 
#they need to hit. Write a query that outputs each employee id and whether they hit the quota or not ('yes' or 'no'). Order the results by employee id in ascending order.
import numpy as np
df_deals = pd.read_sql_query('select * from wmq_deals',Conn)
df_sales_quota = pd.read_sql_query('select * from wmq_sales_quotas',Conn)
df_merged = df_deals.merge(df_sales_quota,on='employee_id')
df_merged= df_merged.groupby(['employee_id','quota'])['deal_size'].sum().reset_index()
df_merged['hit_or_miss'] = np.where(df_merged['deal_size']>=df_merged['quota'],'yes','no')
df_merged[['employee_id','hit_or_miss']].sort_values('employee_id',ascending=True)

```

## DataLemur Medium Questions using Python


```python
#31. Uber's Third Transaction
df_transactions = pd.read_sql_query('select * from m_transactions',Conn)
df_transactions['rank'] = df_transactions.groupby('user_id')['transaction_date'].rank(method='dense',ascending=True)
df_transactions[df_transactions['rank']==3][['user_id','spend','transaction_date']]
```


```python
#32.Sending Vs Opening Snapchat
import numpy as np
df_activities = pd.read_sql_query('select * from m_so_activities',Conn)
df_agebreakdown = pd.read_sql_query('select * from m_so_age_breakdown',Conn)
df_merged = df_activities.merge(df_agebreakdown,on='user_id')
df_merged_pivot = df_merged[df_merged['activity_type']!='chat'].pivot_table(values='time_spent',index='age_bucket',columns='activity_type',aggfunc = 'sum').reset_index()
df_merged_pivot['send_prct'] = (100*df_merged_pivot['send']/(df_merged_pivot['open']+df_merged_pivot['send'])).round(2)
df_merged_pivot['open_prct'] = (100*df_merged_pivot['open']/(df_merged_pivot['open']+df_merged_pivot['send'])).round(2)
df_merged_pivot[['age_bucket','send_prct','open_prct']]
```


```python
#33.Tweets'Rolling Averages
df = pd.read_sql_query('select * from m_tra_tweets',Conn)
df['ld'] = df.groupby('user_id')['tweet_count'].rolling(window=3, min_periods=1).mean().reset_index(level=0, drop=True).round(2)
df
```


```python
#34. Highest Grossing Items
#Assume you're given a table containing data on Amazon customers and their spending on products in different category, write a 
#query to identify the top two highest-grossing products within each category in the year 2022. The output should include the 
#category, product, and total spend

df_product = pd.read_sql_query('select * from m_product_spend',Conn)
df_product = df_product[df_product['transaction_date'].dt.year==2022].groupby(['category','product'])['spend'].sum().reset_index(name = 'total_spend')
df_product['rnk'] = df_product.groupby('category')['total_spend'].rank(method='dense',ascending=False)
df_product[df_product['rnk']<=2][['category','product','total_spend']]
```


```python
#35. Top 5 Artists
df_artist = pd.read_sql_query('select * from m_t_a_artists',Conn)
df_songs = pd.read_sql_query('select * from m_t_a_songs',Conn)
df_global_song_rank = pd.read_sql_query('select * from m_t_a_global_song_rank',Conn)

df1 = df_artist.merge(df_songs,on='artist_id')
df_merged = df1.merge(df_global_song_rank,on='song_id')

df_merged = df_merged[df_merged['rank']<=10].groupby('artist_name')['song_id'].count().reset_index(name='song_count')
df_merged['rnk'] = df_merged['song_count'].rank(method='dense',ascending=False)
df_merged[df_merged['rnk']<=5][['artist_name','rnk']].sort_values('rnk',ascending=True)
```


```python
#36. Signup Activation Rate
df_emails = pd.read_sql_query('select * from m_sar_emails',Conn)
df_texts = pd.read_sql_query('select * from m_sar_texts',Conn)
df_merged = df_emails.merge(df_texts, how = 'left', on='email_id')
activation_rate = df_merged[df_merged['signup_action'] == 'Confirmed']['email_id'].nunique() / df_merged['email_id'].nunique()
round(activation_rate,2)

```


```python
#38. Spotify Streaming History(Good Question)
df_history = pd.read_sql_query('select * from m_ssh_songs_history',Conn)
df_weekly = pd.read_sql_query('select * from m_ssh_songs_weekly',Conn)
df_weekly = df_weekly[df_weekly['listen_time'] <= '2022-08-04 23:59:59']
df_weekly = df_weekly.groupby(['user_id','song_id'])['listen_time'].count().reset_index(name = 'song_plays_weekly')
df_merged = df_weekly.merge(df_history,how='outer',on=['user_id','song_id'])
df_merged = df_merged.fillna(0)
df_merged['song_plays'] = df_merged['song_plays'] + df_merged['song_plays_weekly']
df_merged[['user_id','song_id','song_plays']].sort_values('song_plays',ascending=False)
```


```python
#40. Pharmacy Analytics(Part-4)
df_pharmacy = pd.read_sql_query('select * from pharmacy_sales',Conn)
df_pharmacy = df_pharmacy.groupby(['manufacturer','drug'])['units_sold'].sum().reset_index(name='total_units_sold')
df_pharmacy['rank'] = df_pharmacy.groupby('manufacturer')['total_units_sold'].rank(method='dense',ascending=False)
df_pharmacy[df_pharmacy['rank']<=2][['manufacturer','drug']]
```


```python
#41. Frequently Purchased Pairs
df_transaction = pd.read_sql_query('select * from product_transactions',Conn)
result_df = df_transaction.groupby('transaction_date')['product_id'].agg(lambda x: ','.join(x.astype(str))).reset_index().sort_values('product_id')
result_df = result_df[result_df['product_id'].str.count(',') > 0]['product_id'].unique()
result_df
```


```python
#42. Supercloud  Customer
df_customercontract = pd.read_sql_query('select * from m_sc_customer_contracts',Conn)
df_products = pd.read_sql_query('select * from m_sc_products',Conn)

df_merged = df_customercontract.merge(df_products,on='product_id')
df_merged = df_merged.groupby('customer_id')['product_category'].nunique().reset_index(name = 'total_products')
df_merged = df_merged[df_merged['total_products'] == (df_products['product_category'].nunique())]['customer_id']
df_merged
```


```python
#43. Odd and Even Measurements
df_measurements = pd.read_sql_query('select * from m_oem_measurements',Conn)
df_measurements['measurement_day'] = df_measurements['measurement_time'].dt.date
df_measurements['rank']= df_measurements.groupby('measurement_day')['measurement_time'].rank(method='dense',ascending=True)
df_measurements['odd_value'] = np.where(df_measurements['rank']%2!=0, df_measurements['measurement_value'],0)
df_measurements['even_value'] = np.where(df_measurements['rank']%2==0, df_measurements['measurement_value'],0)
df_measurements.groupby('measurement_day').agg(odd_sum = ('odd_value','sum'),even_sum = ('even_value','sum')).reset_index()
```


```python
#44. Booking Referral Source
df_bookings = pd.read_sql_query('select * from m_brs_bookings',Conn)
df_bookingattr = pd.read_sql_query('select * from m_brs_booking_attribution',Conn)
df_merged = df_bookingattr.merge(df_bookings,on='booking_id')
df_merged['rnk'] = df_merged.groupby('user_id')['booking_date'].rank(method='first',ascending=True)
df_merged['channel'].fillna('None',inplace=True)
df_merged = df_merged[df_merged['rnk'] == 1].groupby(['channel','rnk'])['booking_id'].count().reset_index(name='cnt')
df_merged['prct'] = (100*df_merged['cnt']/df_merged['cnt'].sum()).round(2)
df_merged[(df_merged['cnt'] == df_merged['cnt'].max()) &(df_merged['channel']!='None')][['channel','prct']]
```


```python
#45. Shopping Spree
df_transactions = pd.read_sql_query('select * from m_uss_transactions',Conn)
df_transactions = df_transactions.sort_values(by=['user_id', 'transaction_date'], ascending=[True, True])
df_transactions['diff1'] = (df_transactions.groupby('user_id')['transaction_date'].shift(-1) - df_transactions['transaction_date']).dt.days
df_transactions['diff2'] = (df_transactions.groupby('user_id')['transaction_date'].shift(-2) - df_transactions.groupby('user_id')['transaction_date'].shift(-1)).dt.days
df_transactions[(df_transactions['diff1']==1)&(df_transactions['diff2'] ==1)]['user_id']
```


```python
#46.2nd Ride Delay
df_users = pd.read_sql_query('select * from m_rd_users',Conn)
df_rides = pd.read_sql_query('select * from m_rd_rides',Conn)
df_merged = df_users.merge(df_rides,on='user_id')
df_merged['rnk'] = df_merged.groupby('user_id')['ride_date'].rank(method='first',ascending=True)
user_id_in_the_moment = df_merged[(df_merged['rnk']==1) & (df_merged['registration_date'] == df_merged['ride_date'])]['user_id']
df_merged = df_merged[df_merged['user_id'].isin(user_id_in_the_moment)]
df_merged = df_merged[df_merged['rnk'] == 2]
ride_delay = round(((df_merged['ride_date'].dt.day - df_merged['registration_date'].dt.day).sum() / df_merged['ride_id'].count()),2)
ride_delay
```


```python
#47. Histogram of Users and Purchases
df_transactions = pd.read_sql_query('select * from m_hup_user_transactions',Conn)
df_transactions['rnk'] = df_transactions.groupby('user_id')['transaction_date'].rank(method='dense',ascending=False)
df_transactions = df_transactions[df_transactions['rnk'] == 1].groupby(['user_id','transaction_date'])['product_id'].count().reset_index(name='product_count')
df_transactions.sort_values('transaction_date')
```


```python
#48. Google Maps Flagged UGC
df_placeinfo = pd.read_sql_query('select * from m_gmf_place_info',Conn)
df_mapsreview = pd.read_sql_query('select * from m_gmf_maps_ugc_review',Conn)
df_merged = df_placeinfo.merge(df_mapsreview,on='place_id')
df_merged = df_merged[df_merged['content_tag'].str.lower() == 'off-topic'].groupby('place_category')['content_tag'].count().reset_index(name = 'total_tags')
df_merged['rnk'] = df_merged['total_tags'].rank(method= 'dense',ascending=False)
df_merged[df_merged['rnk']==1]['place_category'].sort_values()
```


```python
#49. Compressed Mode
df_items = pd.read_sql_query('select * from  items_per_order',Conn)
df_items = df_items.groupby('item_count')['order_occurrences'].sum().reset_index(name = 'order_occurrences')
df_items['occurence_count'] = df_items['order_occurrences'].rank(method = 'dense',ascending=False)
df_items[df_items['occurence_count'] == 1]['item_count']
```


```python
#50. Card Launch
df_cardsissued = pd.read_sql_query('select * from monthly_cards_issued',Conn)
df_cardsissued = df_cardsissued.groupby(['card_name','issue_year','issue_month'])['issued_amount'].sum().reset_index(name='total_amount')
df_cardsissued['issue_date'] = df_cardsissued['issue_year'].astype(str)+'-'+df_cardsissued['issue_month'].astype(str)
df_cardsissued['rnk'] = df_cardsissued.groupby('card_name')['issue_date'].rank(method='dense',ascending=True)
df_cardsissued[df_cardsissued['rnk']==1][['card_name','total_amount']].sort_values('total_amount',ascending=False)
```


```python
#51. International Call Percentage
df_phonecalls = pd.read_sql_query('select * from phone_calls',Conn)
df_phoneinfo = pd.read_sql_query('select * from m_icp_phone_info',Conn)
df_merged_caller = df_phonecalls.merge(df_phoneinfo,on='caller_id')
df_merged = df_merged_caller.merge(df_phoneinfo, left_on='receiver_id_x', right_on = 'caller_id')
call_prct = (100*df_merged[df_merged['country_id_x']!=df_merged['country_id_y']]['caller_id_x'].count() / (df_phonecalls['caller_id'].count())).round(1)
call_prct
```


```python
#52. LinkedIn Power Creators(Part 2)
df_personalprofile = pd.read_sql_query('select * from m_lpc_personal_profiles',Conn)
df_employee = pd.read_sql_query('select * from m_lpc_employee_company',Conn)
df_companypages = pd.read_sql_query('select * from m_lpc_company_pages',Conn)
df_merged = df_personalprofile.merge(df_employee,left_on = 'profile_id',right_on = 'personal_profile_id')
df_merged = df_merged.merge(df_companypages, on ='company_id')
df_merged['rnk'] = df_merged.groupby('profile_id')['followers_y'].rank(method='dense',ascending=False)
df_merged[(df_merged['rnk']==1)&(df_merged['followers_x']>df_merged['followers_y'])]['profile_id'].drop_duplicates()
```


```python
#53. Unique Money Transfer
df_payments = pd.read_sql_query('select * from m_umtp_payments',Conn)
df_merged = df_payments.merge(df_payments, left_on = ['payer_id','recipient_id'],right_on=['recipient_id','payer_id'])
df_merged = df_merged[['payer_id_x','recipient_id_x']].drop_duplicates()
df_merged['payer_id_x'].count() / 2

```


```python
#54. User Session Activity
df_session = pd.read_sql_query('select * from m_usa_sessions',Conn)
df_session = df_session[(df_session['start_date'] >='2022-01-01')&(df_session['start_date']<='2022-02-01')].groupby(['session_type','user_id'])['duration'].sum().reset_index(name='total_duration')
df_session['rnk'] = df_session.groupby('session_type')['total_duration'].rank(method='dense',ascending=False)
df_session[['user_id','session_type','rnk']].sort_values(['session_type','rnk'])

```


```python
#55. First Transaction
df_transactions = pd.read_sql_query('select * from m_ft_user_transactions',Conn)
df_transactions = df_transactions.groupby(['user_id','transaction_date'])['spend'].sum().reset_index(name = 'total_spend')
df_transactions['rnk'] = df_transactions.groupby('user_id')['transaction_date'].rank(method='dense')
df_transactions[(df_transactions['rnk']==1) & (df_transactions['total_spend']>=50)]['user_id'].count()
```


```python
#56. Email Table Transaction
#Each Facebook user can designate a personal email address, a business email address, and a recovery email address.Unfortunately
#the table is currently in the wrong format, so you need to transform its structure to show the following columns (see example 
#output): user id, personal email, business email, and recovery email. Sort your answer by user id in ascending order.
df_users = pd.read_sql_query('select * from m_ett_users',Conn)
pivoted_df = df_users.pivot(index='user_id', columns='email_type', values='email')
pivoted_df.reset_index()
```


```python
#57. Photoshop Revenue Analysis
#For every customer that bought Photoshop, return a list of the customers, and the total spent on all the products except for 
#Photoshop products.Sort your answer by customer ids in ascending order.
df_transactions = pd.read_sql_query('select * from m_pra_adobe_transactions',Conn)
customer_ids = df_transactions[df_transactions['product'].str.lower()=='photoshop']['customer_id']
df_transactions[(df_transactions['customer_id'].isin(customer_ids))&(df_transactions['product'].str.lower()!='photoshop')].groupby('customer_id')['revenue'].sum().reset_index().sort_values('customer_id')
```


```python
#58. Cumulative Purchase by Product Type
df_transactions = pd.read_sql_query('select * from m_cppt_total_trans',Conn)
df_transactions = df_transactions.sort_values('order_date')
df_transactions['total_sum'] = df_transactions.groupby('product_type')['quantity'].cumsum()
df_transactions[['order_date','product_type','total_sum']]
```


```python
#59.Invalid Search Results
df_category = pd.read_sql_query('select * from m_isr_search_category',Conn)
df_category['invalid_search'] = df_category['num_search']*df_category['invalid_result_pct']*0.01
df_category = df_category[df_category['invalid_result_pct'].notnull()].groupby('country').agg({'num_search':'sum','invalid_search':'sum'}).reset_index()
df_category['overall_invalid_prct'] = 100.0*df_category['invalid_search']/df_category['num_search'] 
df_category[['country','num_search','overall_invalid_prct']].round(2).sort_values('country')
```


```python
#60. Repeat Purchases on Multiple Days
df_purchases = pd.read_sql_query('select * from m_rpmd_purchases',Conn)
df_purchases['purchase_date'] = df_purchases['purchase_date'].dt.date
df_purchases = df_purchases.groupby(['user_id','product_id'])['purchase_date'].nunique().reset_index(name='total_orders')
df_purchases = df_purchases[df_purchases['total_orders']>1]
df_purchases['user_id'].nunique()
```


```python
#61. Compensation Outliers
df_employee = pd.read_sql_query('select * from m_co_employee_pay',Conn)
df_employee['average'] = df_employee.groupby(['title'])['salary'].transform('mean')
df_employee = df_employee[(df_employee['salary'] > df_employee['average']*2) | (df_employee['salary'] < df_employee['average']*0.5)]
df_employee['decision'] = np.where(df_employee['salary'] > 2*df_employee['average'] ,'Overpaid','Underpaid')
df_employee
```


```python
#62. Y-on-Y Growth Rate
df_transactions = pd.read_sql_query('select * from h_ygr_user_transactions',Conn)
df_transactions['transaction_date'] = df_transactions['transaction_date'].dt.year
df_transactions = df_transactions.groupby(['product_id','transaction_date'])['spend'].sum().reset_index(name='current_year_spend')
df_transactions.sort_values(['product_id','transaction_date'],inplace=True)
df_transactions['previous_year_spend'] = df_transactions.groupby('product_id')['current_year_spend'].shift(1)
df_transactions['yoy'] = (100*(df_transactions['current_year_spend'] - df_transactions['previous_year_spend'])/df_transactions['previous_year_spend']).round(2)
df_transactions
```


```python
#63. Consecutive Filing Years
df_taxes = pd.read_sql_query('select * from filed_taxes',Conn)
df_taxes = df_taxes[df_taxes['product'].str.contains('turbotax', case=False)]
df_taxes['filing_date'] = df_taxes['filing_date'].dt.year
df_taxes.sort_values(['user_id','filing_date'],inplace=True)
df_taxes['ld1'] = df_taxes.groupby('user_id')['product'].shift(-1)
df_taxes['ld2'] = df_taxes.groupby('user_id')['product'].shift(-2)
df_taxes[(df_taxes['ld1'].isna()==False)&(df_taxes['ld2'].isna()==False)]['user_id'].drop_duplicates()
```


```python

```


```python

```


```python

```


```python

```


```python

```


```python

```
