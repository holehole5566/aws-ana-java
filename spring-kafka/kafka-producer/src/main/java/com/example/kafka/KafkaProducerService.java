package com.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class KafkaProducerService {
    
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Value("${kafka.topic}")
    private String topic;
    
    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    public void sendMessage(String message) {
        String key = UUID.randomUUID().toString();
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, message);
        sendSync(record);
    }
    
    private void sendSync(ProducerRecord<String, Object> producerRecord) {
        String traceId = ensureTraceHeader(producerRecord);
        try {
            log.info("[sendSync] START - topic={}, key={}, trace={}", producerRecord.topic(), producerRecord.key(), traceId);
            
            // 檢查 metadata
            checkMetadata(producerRecord.topic());
            
            long startTime = System.currentTimeMillis();
            
            CompletableFuture<SendResult<String, Object>> future = doSendAsync(producerRecord, traceId);
            log.info("[sendSync] Future created, now calling future.get() to WAIT for result...");
            
            future.get();
            
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            log.info("[sendSync] COMPLETED - topic={}, key={}, trace={}, totalTime={}ms", 
                    producerRecord.topic(), producerRecord.key(), traceId, totalTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error("[sendSync] INTERRUPTED - topic={}, key={}", producerRecord.topic(), producerRecord.key(), ie);
            throw new BusinessException("kafka interrupted exception", ie);
        } catch (Exception e) {
            log.error("[sendSync] FAILED - topic={}, key={}", producerRecord.topic(), producerRecord.key(), e);
            throw new BusinessException("failed to send kafka", e);
        }
    }

    private CompletableFuture<SendResult<String, Object>> doSendAsync(
            ProducerRecord<String, Object> producerRecord, String traceId) {

        log.info("[doSendAsync] Calling kafkaTemplate.send() - topic={}, key={}, trace={}",
                producerRecord.topic(), producerRecord.key(), traceId);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(producerRecord);
        
        log.info("[doSendAsync] kafkaTemplate.send() returned immediately, registering callback...");
        
        future.whenComplete((res, ex) -> {
            if (ex != null) {
                log.error("[whenComplete] CALLBACK FAILED - topic={}, key={}, trace={}, error={}",
                        producerRecord.topic(), producerRecord.key(), traceId, ex.getMessage(), ex);
            } else {
                log.info("[whenComplete] CALLBACK SUCCESS - topic={}, key={}, trace={}, partition={}, offset={}",
                        res.getRecordMetadata().topic(), producerRecord.key(), traceId,
                        res.getRecordMetadata().partition(), res.getRecordMetadata().offset());
            }
        });
        
        log.info("[doSendAsync] Returning future object (async operation in progress)...");
        return future;
    }
    
    private String ensureTraceHeader(ProducerRecord<String, Object> producerRecord) {
        return UUID.randomUUID().toString();
    }
    
    private void checkMetadata(String topic) {
        try {
            log.debug("[checkMetadata] Fetching metadata for topic: {}", topic);
            
            // 使用 partitionsFor 來觸發 metadata 請求
            List<PartitionInfo> partitions = kafkaTemplate.partitionsFor(topic);
            
            if (partitions == null || partitions.isEmpty()) {
                log.warn("[checkMetadata] No partitions found for topic: {}", topic);
            } else {
                log.debug("[checkMetadata] Topic: {}, Partition count: {}", topic, partitions.size());
                for (PartitionInfo partition : partitions) {
                    log.debug("[checkMetadata] Partition: {}, Leader: {}, Replicas: {}, ISR: {}",
                            partition.partition(),
                            partition.leader(),
                            partition.replicas().length,
                            partition.inSyncReplicas().length);
                }
            }
        } catch (Exception e) {
            log.error("[checkMetadata] Failed to fetch metadata for topic: {}, error: {}", topic, e.getMessage(), e);
        }
    }
}
