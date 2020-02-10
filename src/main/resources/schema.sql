drop table if exists topic_message;
create table if not exists topic_message
(
    consensus_timestamp bigint primary key not null,
    realm_num           int            not null,
    topic_num           int                  not null,
    message             bytea                        null,
    running_hash        bytea                        null,
    sequence_number     bigint                       null
);

create index if not exists topic_message__realm_num_timestamp
    on topic_message (realm_num, topic_num, consensus_timestamp);
