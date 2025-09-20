// GROUP MEMBERS: (User)-[:MEMBER_OF]->(Community)
MATCH (u:User {user_id: $user_id}), (c:Community {community_id: $community_id})
MERGE (u)-[:MEMBER_OF {joined_at: $joined_at}]->(c);
