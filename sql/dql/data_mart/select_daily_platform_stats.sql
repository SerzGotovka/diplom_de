WITH b AS (
    SELECT
        date_trunc('day', %(as_of_date)s::timestamp)                      AS d0,
        date_trunc('day', %(as_of_date)s::timestamp) + INTERVAL '1 day'   AS d1,
        date_trunc('day', %(as_of_date)s::timestamp) - INTERVAL '6 days'  AS w0,
        date_trunc('day', %(as_of_date)s::timestamp) - INTERVAL '29 days' AS m0
),

-- суточные счётчики (строго за [d0; d1))
posts_day AS (
    SELECT COUNT(*)::bigint AS c
    FROM dds.posts p JOIN b ON p.created_at >= b.d0 AND p.created_at < b.d1
),
comments_day AS (
    SELECT COUNT(*)::bigint AS c
    FROM dds.comments c JOIN b ON c.created_at >= b.d0 AND c.created_at < b.d1
),
reactions_day AS (
    SELECT COUNT(*)::bigint AS c
    FROM dds.reactions r JOIN b ON r.created_at >= b.d0 AND r.created_at < b.d1
),

-- активные пользователи: день/неделя/30 дней (окна [w0; d1), [m0; d1))
active_d AS (
    SELECT DISTINCT x.user_id
    FROM (
        SELECT p.user_id FROM dds.posts p     JOIN b ON p.created_at >= b.d0 AND p.created_at < b.d1
        UNION ALL
        SELECT c.user_id FROM dds.comments c  JOIN b ON c.created_at >= b.d0 AND c.created_at < b.d1
        UNION ALL
        SELECT r.user_id FROM dds.reactions r JOIN b ON r.created_at >= b.d0 AND r.created_at < b.d1
    ) x
),
active_w AS (
    SELECT DISTINCT x.user_id
    FROM (
        SELECT p.user_id FROM dds.posts p     JOIN b ON p.created_at >= b.w0 AND p.created_at < b.d1
        UNION ALL
        SELECT c.user_id FROM dds.comments c  JOIN b ON c.created_at >= b.w0 AND c.created_at < b.d1
        UNION ALL
        SELECT r.user_id FROM dds.reactions r JOIN b ON r.created_at >= b.w0 AND r.created_at < b.d1
    ) x
),
active_m AS (
    SELECT DISTINCT x.user_id
    FROM (
        SELECT p.user_id FROM dds.posts p     JOIN b ON p.created_at >= b.m0 AND p.created_at < b.d1
        UNION ALL
        SELECT c.user_id FROM dds.comments c  JOIN b ON c.created_at >= b.m0 AND c.created_at < b.d1
        UNION ALL
        SELECT r.user_id FROM dds.reactions r JOIN b ON r.created_at >= b.m0 AND r.created_at < b.d1
    ) x
)

SELECT
    %(as_of_date)s::date AS dt,
    (SELECT c FROM posts_day)     AS total_posts,
    (SELECT c FROM comments_day)  AS total_comments,
    (SELECT c FROM reactions_day) AS total_reactions,
    ((SELECT c FROM comments_day) + (SELECT c FROM reactions_day))::float
        / NULLIF((SELECT c FROM posts_day), 0)                             AS interactions_per_post,
    (SELECT COUNT(*) FROM active_d)                                        AS dau,
    (SELECT COUNT(*) FROM active_w)                                        AS wau,
    (SELECT COUNT(*) FROM active_m)                                        AS mau,
    (SELECT COUNT(*) FROM active_d)::float / NULLIF((SELECT COUNT(*) FROM active_w), 0) AS dau_wau_ratio,
    (SELECT COUNT(*) FROM active_w)::float / NULLIF((SELECT COUNT(*) FROM active_m), 0) AS wau_mau_ratio;
