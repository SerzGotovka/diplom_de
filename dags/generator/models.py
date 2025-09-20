
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class User(BaseModel):
    type: str = "user"
    user_id: str
    name: str
    created_at: datetime

class Friend(BaseModel):
    type: str = "friend"
    user_id: str
    friend_id: str
    created_at: datetime

class Post(BaseModel):
    type: str = "post"
    post_id: str
    user_id: str
    text: str
    created_at: datetime

class Comment(BaseModel):
    type: str = "comment"
    comment_id: str
    post_id: str
    user_id: str
    text: str
    created_at: datetime

class Like(BaseModel):
    type: str = "like"
    like_id: str
    user_id: str
    target_type: str
    target_id: str
    created_at: datetime

class Reaction(BaseModel):
    type: str = "reaction"
    reaction_id: str
    user_id: str
    target_type: str
    target_id: str
    reaction: str
    created_at: datetime

class Community(BaseModel):
    type: str = "community"
    community_id: str
    title: str
    created_at: datetime

class GroupMember(BaseModel):
    type: str = "group_member"
    community_id: str
    user_id: str
    joined_at: datetime

class Media(BaseModel):
    type: str = "media"
    media_id: str
    media_type: str
    url: str
    attached_to_post: Optional[str]
    created_at: datetime

class PinnedPost(BaseModel):
    type: str = "pinned_post"
    community_id: str
    post_id: str
