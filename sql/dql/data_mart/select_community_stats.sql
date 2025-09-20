WITH params AS (
    SELECT
        date_trunc('day', %(as_of_date)s::timestamp) AS d0,
        date_trunc('day', %(as_of_date)s::timestamp) + INTERVAL '1 day' AS d1
)
SELECT
    %(as_of_date)s::date AS dt,
    '__all__'::varchar   AS community_id,

    (SELECT COUNT(*)::bigint
     FROM dds.group_members gm
     JOIN params p ON gm.joined_at >= p.d0 AND gm.joined_at < p.d1
    ) AS new_members,

    (SELECT COUNT(*)::bigint
     FROM dds.posts ps
     JOIN params p ON ps.created_at >= p.d0 AND ps.created_at < p.d1
    ) AS posts,

    (SELECT COUNT(*)::bigint
     FROM dds.comments c
     JOIN params p ON c.created_at >= p.d0 AND c.created_at < p.d1
    ) AS comments,

    (SELECT COUNT(*)::bigint
     FROM dds.reactions r
     JOIN params p ON r.created_at >= p.d0 AND r.created_at < p.d1
    ) AS reactions;
