import random
from datetime import datetime

from faker import Faker

from .models import User, Friend, Post, Comment, Like, Reaction, Community, GroupMember, Media, PinnedPost

faker = Faker()

def generate_user(user_id=None, created_at=None):
    return User(
        user_id=user_id or f"user_{faker.unique.random_int(1, 10000)}",
        name=faker.name(),
        created_at=created_at or datetime.now()
    )

def generate_friend(user_ids, created_at=None):
    u1, u2 = random.sample(user_ids, 2)
    return Friend(
        user_id=u1,
        friend_id=u2,
        created_at=created_at or datetime.now()
    )

def generate_post(user_id, created_at=None):
    return Post(
        post_id=f"post_{faker.unique.random_int(1, 100000)}",
        user_id=user_id,
        text=faker.text(max_nb_chars=140),
        created_at=created_at or datetime.now()
    )

def generate_comment(user_id, post_id, created_at=None):
    return Comment(
        comment_id=f"comment_{faker.unique.random_int(1, 100000)}",
        post_id=post_id,
        user_id=user_id,
        text=faker.text(max_nb_chars=100),
        created_at=created_at or datetime.now()
    )

def generate_like(user_id, target_type, target_id, created_at=None):
    return Like(
        like_id=f"like_{faker.unique.random_int(1, 100000)}",
        user_id=user_id,
        target_type=target_type,
        target_id=target_id,
        created_at=created_at or datetime.now()
    )

def generate_reaction(user_id, target_type, target_id, reaction_type, created_at=None):
    return Reaction(
        reaction_id=f"reaction_{faker.unique.random_int(1, 100000)}",
        user_id=user_id,
        target_type=target_type,
        target_id=target_id,
        reaction=reaction_type,
        created_at=created_at or datetime.now()
    )

def generate_community(created_at=None):
    return Community(
        community_id=f"community_{faker.unique.random_int(1, 10000)}",
        title=faker.company(),
        created_at=created_at or datetime.now()
    )

def generate_group_member(community_id, user_id, joined_at=None):
    return GroupMember(
        community_id=community_id,
        user_id=user_id,
        joined_at=joined_at or datetime.now()
    )

def generate_media(post_id=None, created_at=None):
    media_types = ["photo", "video", "album"]
    return Media(
        media_id=f"media_{faker.unique.random_int(1, 100000)}",
        media_type=random.choice(media_types),
        url=faker.image_url(),
        attached_to_post=post_id,
        created_at=created_at or datetime.now()
    )

def generate_pinned_post(community_id, post_id):
    return PinnedPost(community_id=community_id, post_id=post_id)
