package com.github.logart;

import com.google.common.base.Ticker;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.*;

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

    @Test
    public void should_remove_nullable_additional_properties_from_schema() throws RestClientException, IOException {
        SchemaRegistryClient schemaRegistryClientMock = mock(SchemaRegistryClient.class);
        Map<String, ?> config = Map.of(
                ONEOF_FOR_NULLABLES, true,
                SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"
        );
        when(schemaRegistryClientMock.ticker()).thenReturn(Ticker.systemTicker());
        when(schemaRegistryClientMock.registerWithResponse(any(), any(), anyBoolean()))
                .thenAnswer((Answer<RegisterSchemaResponse>) invocationOnMock -> new RegisterSchemaResponse());
        try (KafkaJsonHeaderSchemaSerializer<ObjectWithNullableMap> serializer = new KafkaJsonHeaderSchemaSerializer<>(schemaRegistryClientMock, config)) {
            ObjectWithNullableMap objectWithMap = new ObjectWithNullableMap();
            byte[] serialized = serializer.serialize("test", objectWithMap);
            assertThat(new String(serialized)).isEqualTo("{\"attributes\":null}");
            verify(schemaRegistryClientMock).registerWithResponse(
                    "test-value",
                    new JsonSchema("{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"ObjectWithNullableMap\",\"type\":\"object\",\"properties\":{}}"),
                    false
            );
        }
    }

    @Test
    public void shoduld_not_remove_siblings_of_additional_attributes() throws RestClientException, IOException {
        SchemaRegistryClient schemaRegistryClientMock = mock(SchemaRegistryClient.class);
        Map<String, ?> config = Map.of(
                ONEOF_FOR_NULLABLES, true,
                SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"
        );
        when(schemaRegistryClientMock.ticker()).thenReturn(Ticker.systemTicker());
        when(schemaRegistryClientMock.registerWithResponse(any(), any(), anyBoolean()))
                .thenAnswer((Answer<RegisterSchemaResponse>) invocationOnMock -> new RegisterSchemaResponse());
        try (KafkaJsonHeaderSchemaSerializer<ObjectWithAFewFields> serializer = new KafkaJsonHeaderSchemaSerializer<>(schemaRegistryClientMock, config)) {
            ObjectWithAFewFields objectWithMap = new ObjectWithAFewFields();
            byte[] serialized = serializer.serialize("test", objectWithMap);
            assertThat(new String(serialized)).isEqualTo("{\"regularField\":null,\"attributes\":null,\"nonNullableMap\":{\"key\":\"value\"}}");
            verify(schemaRegistryClientMock).registerWithResponse(
                    "test-value",
                    new JsonSchema("{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"ObjectWithAFewFields\",\"type\":\"object\",\"properties\":{\"regularField\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]}}}"),
                    false
            );
        }
    }
}