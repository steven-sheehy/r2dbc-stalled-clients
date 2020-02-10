package com.example.r2dbc.example;

import java.util.Comparator;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import org.springframework.data.annotation.Id;

@Builder
@Value
public class TopicMessage implements Comparable<TopicMessage> {

    @Id
    private Long consensusTimestamp;

    @ToString.Exclude
    private byte[] message;

    private int realmNum;

    @ToString.Exclude
    private byte[] runningHash;

    private long sequenceNumber;

    private int topicNum;

    @Override
    public int compareTo(TopicMessage other) {
        return Comparator.nullsFirst(Comparator.comparingLong(TopicMessage::getSequenceNumber)).compare(this, other);
    }
}
