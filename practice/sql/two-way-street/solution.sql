

select count(distinct(
  min(sender_id, reply_to) || ',' ||
max(sender_id,reply_to))) as conversation_count

from chat_msgs
where reply_to is not null
