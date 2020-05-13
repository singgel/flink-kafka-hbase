package com.singgel.bigdata.flinksinkhbase.common;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * \* @author singgel
 * \* @created_at: 2019/3/24 下午2:22
 * \
 */
public class CustomJsonDeserializationSchema implements KeyedDeserializationSchema<ObjectNode> {

    private final boolean includeMetadata;
    private ObjectMapper mapper;

    public CustomJsonDeserializationSchema(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    @Override
    public ObjectNode deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        ObjectNode node = mapper.createObjectNode();
        if (messageKey != null) {
            node.put("key", new String(messageKey, "utf-8"));
        }
        if (message != null) {
            node.put("value", new String(message, "utf-8"));
        }
        if (includeMetadata) {
            node.putObject("metadata")
                    .put("offset", offset)
                    .put("topic", topic)
                    .put("partition", partition);
        }
        return node;
    }

    @Override
    public boolean isEndOfStream(ObjectNode nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {
        return getForClass(ObjectNode.class);
    }

}
