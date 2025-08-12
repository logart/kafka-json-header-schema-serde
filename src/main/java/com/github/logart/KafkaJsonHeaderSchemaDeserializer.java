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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.json.JsonSchemaAndValue;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.github.logart.JsonHeaderSchemaConverter.X_JSON_HEADER_SCHEMA_ID;

/**
 * Generic JSON deserializer.
 */
public class KafkaJsonHeaderSchemaDeserializer<T> extends KafkaJsonSchemaDeserializer<T> implements Deserializer<T> {
    private boolean schemaInHeaders;
    private boolean modifyPayload;

    public KafkaJsonHeaderSchemaDeserializer() {
        super();
    }

    public KafkaJsonHeaderSchemaDeserializer(SchemaRegistryClient client) {
        super(client);
    }

    public KafkaJsonHeaderSchemaDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
        super(client, props);
    }

    public KafkaJsonHeaderSchemaDeserializer(SchemaRegistryClient client, Map<String, ?> props, Class<T> type) {
        super(client, props, type);
    }

    @Override
    protected void configure(KafkaJsonSchemaDeserializerConfig config, Class<T> clazz) {
        super.configure(config, clazz);

        Map<String, Object> originals = config.originals();
        schemaInHeaders = getOrDefault(originals, "schema.id.in.headers", Boolean::parseBoolean, Boolean.FALSE);
        modifyPayload = getOrDefault(originals, "modify.payload", Boolean::parseBoolean, Boolean.TRUE);
    }

    // The Object return type is a bit messy, but this is the simplest way to have
    // flexible decoding and not duplicate deserialization code multiple times for different variants.
    @Override
    protected Object deserialize(
            boolean includeSchemaAndVersion, String topic, Boolean isKey, Headers headers, byte[] payload
    ) throws SerializationException, InvalidConfigurationException {
        if (schemaRegistry == null) {
            throw new InvalidConfigurationException(
                    "SchemaRegistryClient not found. You need to configure the deserializer "
                            + "or use deserializer constructor with SchemaRegistryClient.");
        }
        // Even if the caller requests schema & version, if the payload is null we cannot include it.
        // The caller must handle this case.
        if (payload == null) {
            return null;
        }

        int id = -1;
        try {
            ByteBuffer buffer;
            if (modifyPayload) {
                buffer = getByteBuffer(payload);
            } else {
                buffer = ByteBuffer.wrap(payload);
            }
            if (schemaInHeaders) {
                Header header = headers.lastHeader(X_JSON_HEADER_SCHEMA_ID);
                id = header != null
                        ? Integer.parseInt(new String(header.value()))
                        : -1;
            } else if (modifyPayload) {
                id = buffer.getInt();
            }
            String subject = isKey == null || strategyUsesSchema(isKey)
                    ? getContextName(topic) : subjectName(topic, isKey, null);
            JsonSchema schema = ((JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id));
            if (isKey != null && strategyUsesSchema(isKey)) {
                subject = subjectName(topic, isKey, schema);
                schema = schemaForDeserialize(id, schema, subject, isKey);
            }

            ParsedSchema readerSchema = null;
            if (metadata != null) {
                readerSchema = getLatestWithMetadata(subject).getSchema();
            } else if (useLatestVersion) {
                readerSchema = lookupLatestVersion(subject, schema, false).getSchema();
            }
            if (includeSchemaAndVersion || readerSchema != null) {
                Integer version = schemaVersion(topic, isKey, id, subject, schema, null);
                schema = schema.copy(version);
            }
            List<Migration> migrations = Collections.emptyList();
            if (readerSchema != null) {
                migrations = getMigrations(subject, schema, readerSchema);
            }

            int length = buffer.limit();
            if (modifyPayload) {
                length -= 1 + idSize; // 1 magic byte, and 4 bytes for the id
            }
            int start = buffer.position() + buffer.arrayOffset();

            JsonNode jsonNode = null;
            if (!migrations.isEmpty()) {
                jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
                jsonNode = (JsonNode) executeMigrations(migrations, subject, topic, headers, jsonNode);
            }

            if (readerSchema != null) {
                schema = (JsonSchema) readerSchema;
            }
            if (schema.ruleSet() != null && schema.ruleSet().hasRules(RuleMode.READ)) {
                if (jsonNode == null) {
                    jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
                }
                jsonNode = (JsonNode) executeRules(
                        subject, topic, headers, payload, RuleMode.READ, null, schema, jsonNode
                );
            }

            if (validate) {
                try {
                    if (jsonNode == null) {
                        jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
                    }
                    jsonNode = schema.validate(jsonNode);
                } catch (JsonProcessingException | ValidationException e) {
                    throw new SerializationException("JSON "
                            + jsonNode
                            + " does not match schema "
                            + schema.canonicalString(), e);
                }
            }

            Object value;
            if (type != null && !Object.class.equals(type)) {
                value = jsonNode != null
                        ? objectMapper.convertValue(jsonNode, type)
                        : objectMapper.readValue(buffer.array(), start, length, type);
            } else {
                String typeName;
                if (schema.has("oneOf") || schema.has("anyOf") || schema.has("allOf")) {
                    if (jsonNode == null) {
                        jsonNode = objectMapper.readValue(buffer.array(), start, length, JsonNode.class);
                    }
                    typeName = getTypeName(schema.rawSchema(), jsonNode);
                } else {
                    typeName = schema.getString(typeProperty);
                }
                if (typeName != null) {
                    value = jsonNode != null
                            ? deriveType(jsonNode, typeName)
                            : deriveType(buffer, length, start, typeName);
                } else if (Object.class.equals(type)) {
                    value = jsonNode != null
                            ? objectMapper.convertValue(jsonNode, type)
                            : objectMapper.readValue(buffer.array(), start, length, type);
                } else {
                    // Return JsonNode if type is null
                    value = jsonNode != null
                            ? jsonNode
                            : objectMapper.readTree(new ByteArrayInputStream(buffer.array(), start, length));
                }
            }

            if (includeSchemaAndVersion) {
                // Annotate the schema with the version. Note that we only do this if the schema +
                // version are requested, i.e. in Kafka Connect converters. This is critical because that
                // code *will not* rely on exact schema equality. Regular deserializers *must not* include
                // this information because it would return schemas which are not equivalent.
                //
                // Note, however, that we also do not fill in the connect.version field. This allows the
                // Converter to let a version provided by a Kafka Connect source take priority over the
                // schema registry's ordering (which is implicit by auto-registration time rather than
                // explicit from the Connector).

                return new JsonSchemaAndValue(schema, value);
            }

            return value;
        } catch (InterruptedIOException e) {
            throw new TimeoutException("Error deserializing JSON message for id " + id, e);
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Error deserializing JSON message for id " + id, e);
        } catch (RestClientException e) {
            throw toKafkaException(e, "Error retrieving JSON schema for id " + id);
        } finally {
            postOp(payload);
        }
    }

    private String subjectName(String topic, boolean isKey, JsonSchema schemaFromRegistry) {
        return isDeprecatedSubjectNameStrategy(isKey)
                ? null
                : getSubjectName(topic, isKey, null, schemaFromRegistry);
    }

    private Integer schemaVersion(
            String topic, boolean isKey, int id, String subject, JsonSchema schema, Object value
    ) throws IOException, RestClientException {
        Integer version = null;
        if (isDeprecatedSubjectNameStrategy(isKey)) {
            subject = getSubjectName(topic, isKey, value, schema);
        }
        JsonSchema subjectSchema = (JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
        Metadata metadata = subjectSchema.metadata();
        if (metadata != null) {
            version = metadata.getConfluentVersionNumber();
        }
        if (version == null) {
            version = schemaRegistry.getVersion(subject, subjectSchema);
        }
        return version;
    }

    private Object deriveType(JsonNode jsonNode, String typeName) throws IOException {
        try {
            Class<?> cls = Class.forName(typeName);
            return objectMapper.convertValue(jsonNode, cls);
        } catch (ClassNotFoundException e) {
            throw new SerializationException("Class " + typeName + " could not be found.");
        }
    }

    private Object deriveType(
            ByteBuffer buffer, int length, int start, String typeName
    ) throws IOException {
        try {
            Class<?> cls = Class.forName(typeName);
            return objectMapper.readValue(buffer.array(), start, length, cls);
        } catch (ClassNotFoundException e) {
            throw new SerializationException("Class " + typeName + " could not be found.");
        }
    }

    private String getTypeName(Schema schema, JsonNode jsonNode) {
        if (schema instanceof CombinedSchema) {
            for (Schema subschema : ((CombinedSchema) schema).getSubschemas()) {
                boolean valid = false;
                try {
                    JsonSchema.validate(subschema, jsonNode);
                    valid = true;
                } catch (Exception e) {
                    // noop
                }
                if (valid) {
                    return getTypeName(subschema, jsonNode);
                }
            }
        } else if (schema instanceof ReferenceSchema) {
            return getTypeName(((ReferenceSchema) schema).getReferredSchema(), jsonNode);
        }
        return (String) schema.getUnprocessedProperties().get(typeProperty);
    }

    private JsonSchema schemaForDeserialize(
            int id, JsonSchema schemaFromRegistry, String subject, boolean isKey
    ) throws IOException, RestClientException {
        return isDeprecatedSubjectNameStrategy(isKey)
                ? JsonSchemaUtils.copyOf(schemaFromRegistry)
                : (JsonSchema) schemaRegistry.getSchemaBySubjectAndId(subject, id);
    }

    @SuppressWarnings("unchecked")
    private <V> V getOrDefault(Map<String, ?> config, String key, V defaultValue) {
        return getOrDefault(config, key, v -> (V) v, defaultValue);
    }

    private <V> V getOrDefault(Map<String, ?> config, String key, Function<String, V> mapper, V defaultValue) {
        if (config == null || !config.containsKey(key)) {
            return defaultValue;
        }
        return mapper.apply((String) config.get(key));
    }
}