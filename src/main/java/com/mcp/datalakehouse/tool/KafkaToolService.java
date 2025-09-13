package com.mcp.datalakehouse.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

@Service
public class KafkaToolService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaToolService.class);
    private static final Pattern TOPIC_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_.-]+$");
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);
    private static final String UTF8_CLASS_NAME = "org.apache.avro.util.Utf8";

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${schema.registry.url:}")
    private String schemaRegistryUrl;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Tool(
            name = "kafka_topics",
            description = "List all Kafka topics available in the local cluster."
    )
    public Object getKafkaTopics() {
        Properties props = createBasicAdminProperties();
        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsOptions options = new ListTopicsOptions().listInternal(true);
            ListTopicsResult topicsResult = adminClient.listTopics(options);
            KafkaFuture<Set<String>> namesFuture = topicsResult.names();
            Set<String> topics = namesFuture.get();
            logger.info("Kafka topics fetched: {}", topics);
            return Map.of("topics", topics);
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Kafka error fetching topics: {}", e.getMessage());
            return Map.of("error", e.getMessage());
        } catch (Exception e) {
            logger.error("General error fetching topics: {}", e.getMessage());
            return Map.of("error", e.getMessage());
        }
    }

    @Tool(
            name = "peek_kafka_topic",
            description = "Get the latest N messages from a specified Kafka topic. Handles Avro, JSON, and plain text. If maxMessages is not provided, defaults to 10."
    )
    public Object peekKafkaTopic(String topic, Integer maxMessages) {
        try {
            topic = sanitizeTopicName(topic);
            maxMessages = validateMaxMessages(maxMessages);
        } catch (Exception e) {
            logger.error("Invalid parameters for topic '{}': {}", topic, e.getMessage());
            return Map.of("error", e.getMessage());
        }

        // Try Avro first if schema registry is configured, then fallback to String
        if (hasSchemaRegistry()) {
            Object avroResult = null;
            try {
                avroResult = peekMessagesWithDeserializer(topic, maxMessages, true);
            } catch (Exception avroEx) {
                logger.warn("Avro deserialization threw exception for topic '{}', trying String deserializer: {}", topic, avroEx.getMessage());
            }
            // If avroResult is an error map, fallback to string
            if (avroResult instanceof Map && ((Map<?, ?>) avroResult).containsKey("error")) {
                logger.warn("Avro deserialization failed for topic '{}', falling back to String deserializer.", topic);
                return peekMessagesWithDeserializer(topic, maxMessages, false);
            } else if (avroResult != null) {
                return avroResult;
            }
        }

        return peekMessagesWithDeserializer(topic, maxMessages, false);
    }

    private Object peekMessagesWithDeserializer(String topic, int maxMessages, boolean useAvro) {
        Properties props = createConsumerProperties(useAvro);
        List<Map<String, Object>> messages = new ArrayList<>();

        try (KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(partition));

            long endOffset = getEndOffset(consumer, partition);
            long startOffset = Math.max(0, endOffset - maxMessages);
            consumer.seek(partition, startOffset);

            int fetched = 0;
            while (fetched < maxMessages) {
                ConsumerRecords<Object, Object> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) break;

                for (ConsumerRecord<Object, Object> record : records) {
                    messages.add(createMessageMap(record, useAvro));
                    if (++fetched >= maxMessages) break;
                }
            }
        } catch (Exception e) {
            logger.error("Error peeking Kafka topic '{}': {}", topic, e.getMessage());
            return Map.of("error", e.getMessage());
        }

        return Map.of("messages", messages);
    }

    private Map<String, Object> createMessageMap(ConsumerRecord<Object, Object> record, boolean useAvro) {
        Map<String, Object> msg = new HashMap<>();
        msg.put("offset", record.offset());
        msg.put("timestamp", record.timestamp());
        msg.put("partition", record.partition());

        try {
            Object value = record.value();
            if (useAvro && value instanceof GenericRecord) {
                processAvroMessage(msg, (GenericRecord) value);
            } else if (value instanceof String) {
                processStringMessage(msg, (String) value);
            } else {
                msg.put("type", "unknown");
                msg.put("value", value);
            }
        } catch (Exception ex) {
            msg.put("decode_error", ex.getMessage());
        }

        return msg;
    }

    private void processAvroMessage(Map<String, Object> msg, GenericRecord avroRecord) {
        msg.put("type", "avro");
        try {
            Map<String, Object> avroMap = convertAvroRecordToMap(avroRecord);
            msg.put("value", avroMap);
        } catch (Exception avroEx) {
            msg.put("decode_error", "Avro conversion failed: " + avroEx.getMessage());
            msg.put("value", avroRecord.toString());
        }
    }

    private void processStringMessage(Map<String, Object> msg, String value) {
        try {
            Object json = objectMapper.readValue(value, Object.class);
            msg.put("type", "json");
            msg.put("value", json);
        } catch (Exception jsonEx) {
            msg.put("type", "string");
            msg.put("value", value);
        }
    }

    private Map<String, Object> convertAvroRecordToMap(GenericRecord avroRecord) {
        Map<String, Object> avroMap = new HashMap<>();
        for (org.apache.avro.Schema.Field field : avroRecord.getSchema().getFields()) {
            Object fieldValue = avroRecord.get(field.name());
            // Convert Utf8 to String for JSON serialization compatibility
            if (fieldValue != null && UTF8_CLASS_NAME.equals(fieldValue.getClass().getName())) {
                fieldValue = fieldValue.toString();
            }
            avroMap.put(field.name(), fieldValue);
        }
        return avroMap;
    }

    private Properties createBasicAdminProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBootstrapServers);
        return props;
    }

    private Properties createConsumerProperties(boolean useAvro) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, generateConsumerGroupId(useAvro));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        if (useAvro) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            props.put("schema.registry.url", schemaRegistryUrl);
        } else {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }

        return props;
    }

    private String generateConsumerGroupId(boolean useAvro) {
        String prefix = useAvro ? "mcp-peek-avro-" : "mcp-peek-string-";
        return prefix + System.currentTimeMillis();
    }

    private long getEndOffset(KafkaConsumer<Object, Object> consumer, TopicPartition partition) {
        return consumer.endOffsets(Collections.singletonList(partition)).get(partition);
    }

    private boolean hasSchemaRegistry() {
        return schemaRegistryUrl != null && !schemaRegistryUrl.trim().isEmpty();
    }

    private String sanitizeTopicName(String topic) {
        if (topic == null || !TOPIC_NAME_PATTERN.matcher(topic).matches()) {
            throw new IllegalArgumentException("Invalid topic name: " + topic);
        }
        return topic;
    }

    private int validateMaxMessages(Integer maxMessages) {
        if (maxMessages == null || maxMessages <= 0) {
            return 10; // default value
        }
        return maxMessages;
    }
}
