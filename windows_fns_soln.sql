#1.
SELECT title, length,
RANK() OVER(ORDER BY length) AS rank_columns
FROM film
WHERE length IS NOT NULL
;

#2.
SELECT title, length, rating,
RANK() OVER(PARTITION BY rating ORDER BY length DESC) AS rank_columns
FROM film
WHERE length IS NOT NULL;

#3.
WITH no_of_film_actor AS (
SELECT a.actor_id, COUNT(film_id) AS no_of_films FROM actor a 
LEFT JOIN film_actor fa
ON a.actor_id=fa.actor_id
GROUP BY a.actor_id)
SELECT *,
RANK() OVER(PARTITION BY title ORDER BY no_of_films DESC) AS actors_rank
FROM film f
LEFT JOIN film_actor fa
ON f.film_id=fa.film_id
LEFT JOIN no_of_film_actor nf 
ON fa.actor_id= nf.actor_id
WHERE fa.actor_id IS NOT NULL;

#4.
CREATE TEMPORARY TABLE temp_months AS 
SELECT date_format(rental_date,'%m') AS month_new, COUNT(DISTINCT customer_id) AS rental_month_count, COUNT(DISTINCT customer_id)-lag(COUNT(DISTINCT customer_id),1) OVER(ORDER BY (date_format(rental_date,"%m"))) AS prev_rental_month
FROM rental
GROUP BY month_new;
SELECT month_new, rental_month_count, FLOOR((rental_month_count-lag(rental_month_count,1) OVER(ORDER BY month_new))/NULLIF(lag(rental_month_count,1) OVER(ORDER BY month_new),0)*100) AS Percent_change 
FROM temp_months;

#5. 
SELECT DISTINCT customer_id, date_format(rental_date,'%Y-%m') AS month_new FROM rental

;


###5. Calculate the number of retained customers every month, i.e., customers who rented movies in the current and previous months.
WITH CustomerActivity AS (
    -- Step 1: Find the unique month-year of purchase for every customer
    SELECT DISTINCT
        customer_id,
        DATE_FORMAT(rental_date, '%Y-%m') AS purchase_month
    FROM rental
),
RetentionMetrics AS (
    SELECT
        C1.purchase_month AS current_month,
        COUNT(C1.customer_id) AS current_month_customers,
        -- Calculate the previous month's date key for joining
        DATE_FORMAT(
            DATE_SUB(STR_TO_DATE(CONCAT(C1.purchase_month, '-01'), '%Y-%m-%d'), INTERVAL 1 MONTH),
            '%Y-%m'
        ) AS previous_month
    FROM
        CustomerActivity C1
    GROUP BY
        current_month
)
SELECT
    RM.current_month,
    RM.current_month_customers,
    -- Step 3: Count the customers who are present in the current month AND the previous month
    COUNT(C2.customer_id) AS retained_customers_count
FROM
    RetentionMetrics RM
LEFT JOIN
    CustomerActivity C2
    ON RM.previous_month = C2.purchase_month -- Match customers in the previous month
LEFT JOIN
    CustomerActivity C3 
    ON RM.current_month = C3.purchase_month AND C2.customer_id = C3.customer_id -- Match those same customers in the current month
GROUP BY
    RM.current_month, RM.current_month_customers
ORDER BY
    RM.current_month;