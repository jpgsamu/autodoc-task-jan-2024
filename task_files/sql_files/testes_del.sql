SELECT first_sesssion_date, SUM(1)

FROM tb_users

GROUP BY 1
ORDER BY 1