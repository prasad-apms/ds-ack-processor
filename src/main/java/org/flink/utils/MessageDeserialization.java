package org.flink.utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.flink.models.Message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MessageDeserialization implements KafkaRecordDeserializationSchema<Message> {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> receivedMessage, Collector<Message> out) throws IOException {

        Message message = objectMapper.readValue(receivedMessage.value(), Message.class);
   
        if (receivedMessage.key() != null) {
            message.setKey(new String(receivedMessage.key(), StandardCharsets.UTF_8));
        }
        message.setPartition(receivedMessage.partition());
        message.setOffset(receivedMessage.offset());
    
        out.collect(message);
    }

    @Override
    public TypeInformation<Message> getProducedType() {
        return TypeInformation.of(Message.class);
    }
}