/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.github.logart;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.github.logart.JsonHeaderSchemaConverter.X_JSON_HEADER_SCHEMA_ID;

public class KafkaJsonHeaderSchemaSerializer<T> extends KafkaJsonSchemaSerializer<T> implements Serializer<T> {

    private static int DEFAULT_CACHE_CAPACITY = 1000;

    private boolean isKey;
    private Map<ObjectNode, JsonSchema> nodeToSchemaCache;
    private Map<Class<?>, JsonSchema> classToSchemaCache;

    private boolean schemaInHeaders;
    private boolean modifyPayload;

    public KafkaJsonHeaderSchemaSerializer() {
        super();
    }

    public KafkaJsonHeaderSchemaSerializer(SchemaRegistryClient client) {
        super(client);
    }

    public KafkaJsonHeaderSchemaSerializer(SchemaRegistryClient client, Map<String, ?> props) {
        super(client, props);
    }

    public KafkaJsonHeaderSchemaSerializer(SchemaRegistryClient client, Map<String, ?> props, int cacheCapacity) {
        super(client, props, cacheCapacity);
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        super.configure(config, isKey);
        schemaInHeaders = getOrDefault(config, "schema.id.in.headers", Boolean::parseBoolean, Boolean.FALSE);
        modifyPayload = getOrDefault(config, "modify.payload", Boolean::parseBoolean, Boolean.TRUE);
    }

    @Override
    public byte[] serialize(String topic, T record) {
        return serialize(topic, null, record);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T record) {
        if (record == null) {
            return null;
        }
        JsonSchema schema;
        if (JsonSchemaUtils.isEnvelope(record)) {
            schema = nodeToSchemaCache.computeIfAbsent(
                    JsonSchemaUtils.copyEnvelopeWithoutPayload((ObjectNode) record),
                    k -> getSchema(record));
        } else {
            schema = classToSchemaCache.computeIfAbsent(record.getClass(), k -> getSchema(record));
        }
        Object value = JsonSchemaUtils.getValue(record);
        return serializeImpl(
                getSubjectName(topic, isKey, value, schema), topic, headers, (T) value, schema);
    }

    @SuppressWarnings("unchecked")
    protected byte[] serializeImpl(
            String subject,
            String topic,
            Headers headers,
            T object,
            JsonSchema schema
    ) throws SerializationException, InvalidConfigurationException {
        if (schemaRegistry == null) {
            throw new InvalidConfigurationException(
                    "SchemaRegistryClient not found. You need to configure the serializer "
                            + "or use serializer constructor with SchemaRegistryClient.");
        }
        // null needs to treated specially since the client most likely just wants to send
        // an individual null value instead of making the subject a null type. Also, null in
        // Kafka has a special meaning for deletion in a topic with the compact retention policy.
        // Therefore, we will bypass schema registration and return a null value in Kafka, instead
        // of an encoded null.
        if (object == null) {
            return null;
        }
        String restClientErrorMsg = "";
        try {
            int id;
            if (autoRegisterSchema) {
                restClientErrorMsg = "Error registering JSON schema: ";
                io.confluent.kafka.schemaregistry.client.rest.entities.Schema s =
                        registerWithResponse(subject, schema, normalizeSchema);
                if (s.getSchema() != null) {
                    Optional<ParsedSchema> optSchema = schemaRegistry.parseSchema(s);
                    if (optSchema.isPresent()) {
                        schema = (JsonSchema) optSchema.get();
                        schema = schema.copy(s.getVersion());
                    }
                }
                id = s.getId();
            } else if (useSchemaId >= 0) {
                restClientErrorMsg = "Error retrieving schema ID";
                schema = (JsonSchema)
                        lookupSchemaBySubjectAndId(subject, useSchemaId, schema, idCompatStrict);
                id = useSchemaId;
            } else if (metadata != null) {
                restClientErrorMsg = "Error retrieving latest with metadata '" + metadata + "'";
                ExtendedSchema extendedSchema = getLatestWithMetadata(subject);
                schema = (JsonSchema) extendedSchema.getSchema();
                id = extendedSchema.getId();
            } else if (useLatestVersion) {
                restClientErrorMsg = "Error retrieving latest version: ";
                ExtendedSchema extendedSchema = lookupLatestVersion(subject, schema, latestCompatStrict);
                schema = (JsonSchema) extendedSchema.getSchema();
                id = extendedSchema.getId();
            } else {
                restClientErrorMsg = "Error retrieving JSON schema: ";
                id = schemaRegistry.getId(subject, schema, normalizeSchema);
            }
            object = (T) executeRules(subject, topic, headers, RuleMode.WRITE, null, schema, object);
            if (validate) {
                validateJson(object, schema);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            // write schema id to headers, all other implementation stays the same
            if (schemaInHeaders) {
                headers.add(X_JSON_HEADER_SCHEMA_ID, String.valueOf(id).getBytes());
            }
            if (modifyPayload) {
                out.write(MAGIC_BYTE);
                out.write(ByteBuffer.allocate(idSize).putInt(id).array());
            }
            out.write(objectMapper.writeValueAsBytes(object));
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (InterruptedIOException e) {
            throw new TimeoutException("Error serializing JSON message", e);
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Error serializing JSON message", e);
        } catch (RestClientException e) {
            throw toKafkaException(e, restClientErrorMsg + schema);
        } finally {
            postOp(object);
        }
    }

    private JsonSchema getSchema(T record) {
        try {
            return JsonSchemaUtils.getSchema(record, specVersion, scanPackages, oneofForNullables,
                    failUnknownProperties, objectMapper, schemaRegistry);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    private <V> V getOrDefault(Map<String, ?> config, String key, Function<String, V> mapper, V defaultValue) {
        if (config == null || !config.containsKey(key)) {
            return defaultValue;
        }
        return mapper.apply((String) config.get(key));
    }
}