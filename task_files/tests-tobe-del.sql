SELECT session_qty, SUM(1), avg(avg_time_between_sessions) as avg_time_between_sessions
FROM tb_users

GROUP BY 1
ORDER BY 1
