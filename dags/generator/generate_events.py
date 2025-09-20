import random
from datetime import datetime, date, time, timedelta

from .generators import (
    generate_user, generate_friend, generate_post, generate_comment,
    generate_like, generate_reaction, generate_community, generate_group_member,
    generate_media, generate_pinned_post
)
from .writer import write_to_kafka, write_to_minio


def _rand_dt_in_window(start_dt: datetime, end_dt: datetime) -> datetime:
    delta = int((end_dt - start_dt).total_seconds())
    return start_dt + timedelta(seconds=random.randint(0, max(delta - 1, 0)))

def _day_bounds(day: date):
    start = datetime.combine(day, time.min)
    end = start + timedelta(days=1)
    return start, end

def parametrised_all_data_for_day(
    day: date,
    num_users=random.randint(10,50),
    num_communities=random.randint(1,3),
    posts_per_user=random.randint(1,4),
    comments_per_post=random.randint(1,3),
    likes_per_post=random.randint(1,4),
    group_memberships=random.randint(10,30),
    media_per_post=random.randint(1,2),
    pinned_per_community=random.randint(1,2),
    num_reactions=random.randint(30,100)
):
    start, end = _day_bounds(day)

    # Users
    users = [generate_user(created_at=_rand_dt_in_window(start, end)) for _ in range(num_users)]
    user_ids = [u.user_id for u in users]

    # Communities
    communities = [generate_community(created_at=_rand_dt_in_window(start, end)) for _ in range(num_communities)]
    community_ids = [c.community_id for c in communities]

    # Friends (случайные пары)
    friends = [generate_friend(user_ids, created_at=_rand_dt_in_window(start, end)) for _ in range(num_users)]

    # Posts
    posts = []
    for uid in user_ids:
        for _ in range(posts_per_user):
            post = generate_post(uid, created_at=_rand_dt_in_window(start, end))
            posts.append(post)
    post_ids = [p.post_id for p in posts]

    # Comments (после поста)
    comments = []
    for p in posts:
        for _ in range(comments_per_post):
            cts = _rand_dt_in_window(p.created_at, end)
            comments.append(generate_comment(random.choice(user_ids), p.post_id, created_at=cts))
    comment_ids = [c.comment_id for c in comments]

    # Likes (после поста)
    likes = []
    for p in posts:
        for _ in range(likes_per_post):
            lts = _rand_dt_in_window(p.created_at, end)
            likes.append(generate_like(random.choice(user_ids), "post", p.post_id, created_at=lts))

    # Reactions (70% посты / 30% комментарии, после целевого объекта)
    reactions = []
    for _ in range(num_reactions):
        if comment_ids and random.random() < 0.3:
            target_type, tgt_id = "comment", random.choice(comment_ids)
            # найдём время коммента
            base_ts = next(c.created_at for c in comments if c.comment_id == tgt_id)
        else:
            target_type, tgt_id = "post", random.choice(post_ids)
            base_ts = next(p.created_at for p in posts if p.post_id == tgt_id)
        rts = _rand_dt_in_window(base_ts, end)
        reactions.append(
            generate_reaction(
                user_id=random.choice(user_ids),
                target_type=target_type,
                target_id=tgt_id,
                reaction_type=random.choice(["like", "love", "wow", "angry", "sad"]),
                created_at=rts
            )
        )

    # Group members
    group_members = [
        generate_group_member(random.choice(community_ids), random.choice(user_ids),
                              joined_at=_rand_dt_in_window(start, end))
        for _ in range(group_memberships)
    ]

    # Media (после поста)
    media = []
    for p in posts:
        for _ in range(media_per_post):
            media.append(generate_media(p.post_id, created_at=_rand_dt_in_window(p.created_at, end)))

    # Pinned posts
    pinned_posts = [
        generate_pinned_post(random.choice(community_ids), random.choice(post_ids))
        for _ in range(pinned_per_community * num_communities)
    ]

    return {
        "users": [u.dict() for u in users],
        "friends": [f.dict() for f in friends],
        "posts": [p.dict() for p in posts],
        "comments": [c.dict() for c in comments],
        "likes": [l.dict() for l in likes],
        "reactions": [r.dict() for r in reactions],
        "communities": [c.dict() for c in communities],
        "group_members": [g.dict() for g in group_members],
        "media": [m.dict() for m in media],
        "pinned_posts": [p.dict() for p in pinned_posts],
    }


def generate_all_data_and_return(day:str=None):
    # day приходит как 'YYYY-MM-DD' из Airflow
    if day is None:
        dd = datetime.now().date()
    else:
        dd = datetime.strptime(day, "%Y-%m-%d").date()
    return parametrised_all_data_for_day(dd)

def generate_to_kafka(ti):
    data = ti.xcom_pull(task_ids="generate_data")
    for entity, events in data.items():
        for event in events:
            write_to_kafka(entity, event)

def generate_to_minio(ti, day: str = None):
    if day is None:
        dd = datetime.now().date()
    else:
        dd = datetime.strptime(day, "%Y-%m-%d").date()
    data = ti.xcom_pull(task_ids="generate_data")
    events = (
            data["communities"] +
            data["group_members"] +
            data["media"] +
            data["pinned_posts"]
    )
    stamp = datetime.now().strftime('%H%M%S')
    filename = f"batch_{dd}_{stamp}.json"
    write_to_minio(filename, events)
