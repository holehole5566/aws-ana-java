package org.example;

import java.io.Serializable;

public class KafkaMessage implements Serializable {
    private String message;
    private Long timestamp;
    private String partitionDate;

    public KafkaMessage() {
    }

    public KafkaMessage(String message, Long timestamp, String partitionDate) {
        this.message = message;
        this.timestamp = timestamp;
        this.partitionDate = partitionDate;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getPartitionDate() {
        return partitionDate;
    }

    public void setPartitionDate(String partitionDate) {
        this.partitionDate = partitionDate;
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "message='" + message + '\'' +
                ", timestamp=" + timestamp +
                ", partitionDate='" + partitionDate + '\'' +
                '}';
    }
}
