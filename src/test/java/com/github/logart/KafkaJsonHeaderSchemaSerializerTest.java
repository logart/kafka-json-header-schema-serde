package com.github.logart;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class KafkaJsonHeaderSchemaSerializerTest {
    @Test
    public void should_remove_additional_properties_from_schema() throws RestClientException, IOException {
        SchemaRegistryClient schemaRegistryClientMock = mock(SchemaRegistryClient.class);
        try (KafkaJsonHeaderSchemaSerializer<ObjectWithMap> serializer = new KafkaJsonHeaderSchemaSerializer<>(schemaRegistryClientMock)) {
            ObjectWithMap objectWithMap = new ObjectWithMap();
            byte[] serialized = serializer.serialize("test", objectWithMap);
            assertThat(serialized).isEqualTo("{\"attributes\":{\"key1\":\"value1\"}}".getBytes());
            verify(schemaRegistryClientMock).getId(
                    "test-value",
                    new JsonSchema("{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"ObjectWithMap\",\"type\":\"object\",\"properties\":{}}"),
                    false
            );
        }
    }
}