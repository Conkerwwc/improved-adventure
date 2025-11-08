SELECT 
 id, 
 first_name,
 last_name, 
 company,
 city,
 country, 
 phone_1,
 phone_2, 
 email, 
 subscription_date, website
FROM customers
WHERE country IN ('Puerto Rico', 'Switzerland', 'Italy', 'Germany')
ORDER BY first_name, last_name;



\COPY customers(customer_id, first_name, last_name, company, city, country, phone_1, phone_2, email, subscription_date)
FROM 'C:/Users/Desktop/customers.csv'
WITH (FORMAT CSV, HEADER);

       