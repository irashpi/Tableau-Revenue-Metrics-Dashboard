WITH monthly_revenue AS (
    -- Розрахунок доходів по місяцях
    SELECT
        DATE_TRUNC('month', payment_date) AS payment_month,
        user_id,
        game_name,
        SUM(revenue_amount_usd) AS total_revenue
    FROM project.games_payments
    GROUP BY 1, 2, 3
),
revenue_lag_lead AS 
(
    -- Додавання LAG та LEAD для перевірки попередніх та наступних значень
    SELECT 
        mr.payment_month,
        mr.user_id,
        mr.game_name,
        mr.total_revenue,
        LAG(mr.total_revenue) OVER (PARTITION BY mr.user_id ORDER BY mr.payment_month) AS prev_month_revenue,
        LEAD(mr.payment_month) OVER (PARTITION BY mr.user_id ORDER BY mr.payment_month) AS next_payment_month,
        LAG(mr.payment_month) OVER (PARTITION BY mr.user_id ORDER BY mr.payment_month) AS previous_paid_month,
        LEAD(mr.payment_month) OVER (PARTITION BY mr.user_id ORDER BY mr.payment_month) AS next_paid_month
    FROM monthly_revenue mr
),
revenue_metrics AS
(
    -- Розрахунок MRR: new_mrr, expansion_mrr, contraction_mrr
    SELECT
        rll.payment_month,
        rll.user_id,
        rll.game_name,
        rll.total_revenue,
        -- New MRR
        CASE 
            WHEN rll.prev_month_revenue IS NULL THEN rll.total_revenue
            ELSE 0
        END AS new_mrr,
        -- Expansion MRR
        CASE 
            WHEN rll.previous_paid_month IS NOT NULL
                AND rll.payment_month = rll.previous_paid_month + INTERVAL '1 month'
                AND rll.total_revenue > rll.prev_month_revenue THEN rll.total_revenue - rll.prev_month_revenue
            ELSE 0
        END AS expansion_mrr,
        -- Contraction MRR
        CASE 
            WHEN rll.previous_paid_month IS NOT NULL
                AND rll.payment_month = rll.previous_paid_month + INTERVAL '1 month'
                AND rll.total_revenue < rll.prev_month_revenue THEN rll.prev_month_revenue - rll.total_revenue
            ELSE 0
        END AS contraction_mrr,
        -- Churned Revenue
        CASE 
            WHEN rll.next_payment_month IS NULL THEN rll.total_revenue
            WHEN rll.next_payment_month != rll.payment_month + INTERVAL '1 month' THEN rll.total_revenue
            ELSE 0
        END AS churned_revenue
    FROM revenue_lag_lead rll
),
user_metrics AS
(
    -- Підрахунок кількості платних користувачів, нових платних користувачів та ARPPU
    SELECT
        payment_month,
        COUNT(DISTINCT user_id) AS paid_users,  -- Загальна кількість платних користувачів
        COUNT(DISTINCT CASE 
            WHEN prev_month_revenue IS NULL THEN user_id  -- Нові платні користувачі
            ELSE NULL
        END) AS new_paid_users,
        CASE 
            WHEN COUNT(DISTINCT user_id) > 0 THEN SUM(total_revenue) / COUNT(DISTINCT user_id)
            ELSE NULL
        END AS arppu
    FROM revenue_lag_lead
    GROUP BY payment_month
)
SELECT 
    rm.payment_month,
    rm.game_name,  -- Додаємо назву гри
    SUM(rm.total_revenue) AS total_revenue,  -- Додаємо загальний дохід
    SUM(rm.new_mrr) AS new_MRR,
    SUM(rm.expansion_mrr) AS total_expansion_revenue,
    SUM(rm.contraction_mrr) AS total_contraction_revenue,
    SUM(rm.churned_revenue) AS total_churned_revenue,
    um.paid_users,
    um.new_paid_users,  -- Додаємо нових платних користувачів
    um.arppu,
    gpu.language,
    gpu.age,
    gpu.has_older_device_model
FROM revenue_metrics rm
JOIN user_metrics um ON rm.payment_month = um.payment_month
LEFT JOIN project.games_paid_users gpu ON rm.user_id = gpu.user_id  -- Додаємо JOIN з games_paid_users для отримання додаткових даних
GROUP BY rm.payment_month, rm.game_name, um.paid_users, um.new_paid_users, um.arppu, gpu.language, gpu.age, gpu.has_older_device_model
ORDER BY rm.payment_month, rm.game_name;
