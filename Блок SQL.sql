create database final_proect;
UPDATE customer_final SET Gender=null where Gender='';
UPDATE customer_final SET Age=null where Age='';
alter table customer_final modify Age int null;

select * from transactions_final;


create table transactions_final
(date_new date,
Id_check int,
ID_client int,
Count_products decimal(10,3),
Sum_payment decimal(10,2));

load data infile "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\TRANSACTIONS_final.csv"
into table transactions_final
fields terminated by ','
lines terminated by '\n'
ignore 1 rows;


show variables like 'secure_file_priv';
--Задание1
WITH Transactions_Per_Month AS (
    -- Определяем месяцы, в которых клиент совершал покупки
    SELECT 
        ID_client, 
        MONTH(date_new) AS month,
        YEAR(date_new) AS year
    FROM transactions_final
    WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY ID_client, YEAR(date_new), MONTH(date_new)
),
Active_Customers AS (
    -- Оставляем только клиентов с покупками в каждом из 12 месяцев
    SELECT ID_client
    FROM Transactions_Per_Month
    GROUP BY ID_client
    HAVING COUNT(DISTINCT CONCAT(year, '-', LPAD(month, 2, '0'))) = 12
),
Customer_Stats AS (
    -- Вычисляем метрики для клиентов с непрерывной историей покупок
    SELECT 
        t.ID_client,
        COUNT(t.ID_check) AS total_transactions, -- Общее число операций
        SUM(t.Sum_payment) AS total_spent, -- Общая сумма покупок
        SUM(t.Sum_payment) / COUNT(t.ID_check) AS avg_receipt, -- Средний чек
        SUM(t.Sum_payment) / 12 AS avg_monthly_spent -- Средняя сумма покупок за месяц
    FROM transactions_final t
    JOIN Active_Customers ac ON t.ID_client = ac.ID_client
    WHERE t.date_new BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY t.ID_client
)
SELECT 
    c.ID_client,
    c.Total_amount,
    c.Gender,
    c.Age,
    c.Count_city,
    c.Response_communcation,
    c.Communication_3month,
    c.Tenure,
    cs.total_transactions,
    cs.total_spent,
    cs.avg_receipt,
    cs.avg_monthly_spent
FROM Customer_Stats cs
JOIN customer_final c ON cs.ID_client = c.ID_client;


--Задание2
WITH Monthly_Stats AS (
    -- Вычисляем основные метрики по месяцам
    SELECT 
        YEAR(t.date_new) AS year,
        MONTH(t.date_new) AS month,
        COUNT(t.Id_check) AS total_operations,  -- Общее количество операций
        COUNT(DISTINCT t.ID_client) AS unique_clients,  -- Количество уникальных клиентов
        SUM(t.Sum_payment) AS total_revenue,  -- Общая сумма покупок
        SUM(t.Sum_payment) / COUNT(t.Id_check) AS avg_check,  -- Средний чек
        (COUNT(t.Id_check) / SUM(COUNT(t.Id_check)) OVER()) * 100 AS operations_share,  -- Доля операций от общего числа за год
        (SUM(t.Sum_payment) / SUM(SUM(t.Sum_payment)) OVER()) * 100 AS revenue_share  -- Доля выручки месяца от общей выручки
    FROM transactions_final t
    WHERE t.date_new BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY YEAR(t.date_new), MONTH(t.date_new)
),
Gender_Stats AS (
    -- Рассчитываем количество клиентов по полу и их долю затрат
    SELECT 
        YEAR(t.date_new) AS year,
        MONTH(t.date_new) AS month,
        c.Gender,
        COUNT(DISTINCT t.ID_client) AS client_count,  -- Количество клиентов данного пола
        SUM(t.Sum_payment) AS gender_spent,  -- Сумма затрат по полу
        (COUNT(DISTINCT t.ID_client) / SUM(COUNT(DISTINCT t.ID_client)) OVER(PARTITION BY YEAR(t.date_new), MONTH(t.date_new))) * 100 AS gender_ratio,  -- Доля клиентов данного пола
        (SUM(t.Sum_payment) / SUM(SUM(t.Sum_payment)) OVER(PARTITION BY YEAR(t.date_new), MONTH(t.date_new))) * 100 AS gender_spent_share  -- Доля затрат данного пола в месяце
    FROM transactions_final t
    JOIN customer_final c ON t.ID_client = c.ID_client
    WHERE t.date_new BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY YEAR(t.date_new), MONTH(t.date_new), c.Gender
)
-- Финальное объединение данных
SELECT 
    ms.year,
    ms.month,
    ms.avg_check AS avg_check_per_month,  
    ms.total_operations / 12 AS avg_operations_per_month,  
    ms.unique_clients / 12 AS avg_clients_per_month,  
    ms.operations_share AS operations_share_year,  
    ms.revenue_share AS revenue_share_year,  
    gs.Gender,
    gs.client_count,
    gs.gender_ratio,
    gs.gender_spent_share
FROM Monthly_Stats ms
LEFT JOIN Gender_Stats gs ON ms.year = gs.year AND ms.month = gs.month
ORDER BY ms.year, ms.month, gs.Gender;


--Задание3
WITH Age_Groups AS (
    -- Разделяем клиентов по возрастным категориям
    SELECT 
        ID_client,
        CASE 
            WHEN Age IS NULL THEN 'Неизвестно'
            WHEN Age BETWEEN 0 AND 9 THEN '0-9'
            WHEN Age BETWEEN 10 AND 19 THEN '10-19'
            WHEN Age BETWEEN 20 AND 29 THEN '20-29'
            WHEN Age BETWEEN 30 AND 39 THEN '30-39'
            WHEN Age BETWEEN 40 AND 49 THEN '40-49'
            WHEN Age BETWEEN 50 AND 59 THEN '50-59'
            ELSE '60+'
        END AS age_group
    FROM customer_final
),
Total_Stats AS (
    -- Подсчет общей суммы и количества операций за весь период
    SELECT 
        ag.age_group,
        SUM(t.Sum_payment) AS total_sum,
        COUNT(t.ID_check) AS total_operations
    FROM transactions_final t
    JOIN Age_Groups ag ON t.ID_client = ag.ID_client
    GROUP BY ag.age_group
),
Quarterly_Stats AS (
    -- Расчет показателей по кварталам
    SELECT 
        ag.age_group,
        YEAR(t.date_new) AS year,
        QUARTER(t.date_new) AS quarter,
        COUNT(t.ID_check) AS total_operations_q,
        SUM(t.Sum_payment) AS total_sum_q,
        AVG(t.Sum_payment) AS avg_check_q,
        (SUM(t.Sum_payment) / SUM(SUM(t.Sum_payment)) OVER (PARTITION BY YEAR(t.date_new), QUARTER(t.date_new))) * 100 AS revenue_share_q
    FROM transactions_final t
    JOIN Age_Groups ag ON t.ID_client = ag.ID_client
    GROUP BY ag.age_group, YEAR(t.date_new), QUARTER(t.date_new)
)
-- Финальный запрос, объединяющий общие и поквартальные данные
SELECT 
    t.age_group,
    t.total_sum,
    t.total_operations,
    q.year,
    q.quarter,
    q.total_operations_q,
    q.total_sum_q,
    q.avg_check_q,
    q.revenue_share_q
FROM Total_Stats t
LEFT JOIN Quarterly_Stats q ON t.age_group = q.age_group
ORDER BY q.year, q.quarter, t.age_group;







