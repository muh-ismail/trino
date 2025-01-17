/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kafka.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.trino.plugin.kafka.schema.confluent.KafkaWithConfluentSchemaRegistryQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.ENUM;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.STRING;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static io.trino.decoder.protobuf.ProtobufRowDecoderFactory.DEFAULT_MESSAGE;
import static io.trino.decoder.protobuf.ProtobufUtils.getFileDescriptor;
import static io.trino.decoder.protobuf.ProtobufUtils.getProtoFile;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestKafkaProtobufWithSchemaRegistryMinimalFunctionality
        extends AbstractTestQueryFramework
{
    private static final String RECORD_NAME = "schema";
    private static final int MESSAGE_COUNT = 100;

    private TestingKafka testingKafka;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        return KafkaWithConfluentSchemaRegistryQueryRunner.builder(testingKafka).build();
    }

    @Test
    public void testBasicTopic()
            throws Exception
    {
        String topic = "topic-basic-MixedCase";
        assertTopic(
                topic,
                format("SELECT col_1, col_2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT col_1, col_2, col_3 FROM %s", toDoubleQuoted(topic)),
                false,
                producerProperties());
    }

    @Test
    public void testTopicWithKeySubject()
            throws Exception
    {
        String topic = "topic-Key-Subject";
        assertTopic(
                topic,
                format("SELECT key, col_1, col_2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT key, col_1, col_2, col_3 FROM %s", toDoubleQuoted(topic)),
                true,
                producerProperties());
    }

    @Test
    public void testTopicWithRecordNameStrategy()
            throws Exception
    {
        String topic = "topic-Record-Name-Strategy";
        assertTopic(
                topic,
                format("SELECT key, col_1, col_2 FROM \"%1$s&value-subject=%2$s\"", topic, RECORD_NAME),
                format("SELECT key, col_1, col_2, col_3 FROM \"%1$s&value-subject=%2$s\"", topic, RECORD_NAME),
                true,
                ImmutableMap.<String, String>builder()
                        .putAll(producerProperties())
                        .put(VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName())
                        .buildOrThrow());
    }

    @Test
    public void testTopicWithTopicRecordNameStrategy()
            throws Exception
    {
        String topic = "topic-Topic-Record-Name-Strategy";
        assertTopic(
                topic,
                format("SELECT key, col_1, col_2 FROM \"%1$s&value-subject=%1$s-%2$s\"", topic, RECORD_NAME),
                format("SELECT key, col_1, col_2, col_3 FROM \"%1$s&value-subject=%1$s-%2$s\"", topic, RECORD_NAME),
                true,
                ImmutableMap.<String, String>builder()
                        .putAll(producerProperties())
                        .put(VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
                        .buildOrThrow());
    }

    @Test
    public void testBasicTopicForInsert()
            throws Exception
    {
        String topic = "topic-basic-inserts";
        assertTopic(
                topic,
                format("SELECT col_1, col_2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT col_1, col_2, col_3 FROM %s", toDoubleQuoted(topic)),
                false,
                producerProperties());
        assertQueryFails(
                format("INSERT INTO %s (col_1, col_2, col_3) VALUES ('Trino', 14, 1.4)", toDoubleQuoted(topic)),
                "Insert is not supported for schema registry based tables");
    }

    private Map<String, String> producerProperties()
    {
        return ImmutableMap.of(
                SCHEMA_REGISTRY_URL_CONFIG, testingKafka.getSchemaRegistryConnectString(),
                KEY_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
    }

    private void assertTopic(String topicName, String initialQuery, String evolvedQuery, boolean isKeyIncluded, Map<String, String> producerConfig)
            throws Exception
    {
        testingKafka.createTopic(topicName);

        assertNotExists(topicName);

        List<ProducerRecord<DynamicMessage, DynamicMessage>> messages = createMessages(topicName, MESSAGE_COUNT, true, getInitialSchema(), getKeySchema());
        testingKafka.sendMessages(messages.stream(), producerConfig);

        waitUntilTableExists(topicName);
        assertCount(topicName, MESSAGE_COUNT);

        assertQuery(initialQuery, getExpectedValues(messages, getInitialSchema(), isKeyIncluded));

        List<ProducerRecord<DynamicMessage, DynamicMessage>> newMessages = createMessages(topicName, MESSAGE_COUNT, false, getEvolvedSchema(), getKeySchema());
        testingKafka.sendMessages(newMessages.stream(), producerConfig);

        List<ProducerRecord<DynamicMessage, DynamicMessage>> allMessages = ImmutableList.<ProducerRecord<DynamicMessage, DynamicMessage>>builder()
                .addAll(messages)
                .addAll(newMessages)
                .build();
        assertCount(topicName, allMessages.size());
        assertQuery(evolvedQuery, getExpectedValues(allMessages, getEvolvedSchema(), isKeyIncluded));
    }

    private static String getExpectedValues(List<ProducerRecord<DynamicMessage, DynamicMessage>> messages, Descriptor descriptor, boolean isKeyIncluded)
    {
        StringBuilder valuesBuilder = new StringBuilder("VALUES ");
        ImmutableList.Builder<String> rowsBuilder = ImmutableList.builder();
        for (ProducerRecord<DynamicMessage, DynamicMessage> message : messages) {
            ImmutableList.Builder<String> columnsBuilder = ImmutableList.builder();

            if (isKeyIncluded) {
                addExpectedColumns(message.key().getDescriptorForType(), message.key(), columnsBuilder);
            }

            addExpectedColumns(descriptor, message.value(), columnsBuilder);

            rowsBuilder.add(format("(%s)", String.join(", ", columnsBuilder.build())));
        }
        valuesBuilder.append(String.join(", ", rowsBuilder.build()));
        return valuesBuilder.toString();
    }

    private static void addExpectedColumns(Descriptor descriptor, DynamicMessage message, ImmutableList.Builder<String> columnsBuilder)
    {
        for (FieldDescriptor field : descriptor.getFields()) {
            FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByName(field.getName());
            if (fieldDescriptor == null) {
                columnsBuilder.add("null");
                continue;
            }
            Object value = message.getField(message.getDescriptorForType().findFieldByName(field.getName()));
            if (field.getJavaType() == STRING || field.getJavaType() == ENUM) {
                columnsBuilder.add(toSingleQuoted(value));
            }
            else {
                columnsBuilder.add(String.valueOf(value));
            }
        }
    }

    private void assertNotExists(String tableName)
    {
        if (schemaExists()) {
            assertQueryReturnsEmptyResult(format("SHOW TABLES LIKE '%s'", tableName));
        }
    }

    private void waitUntilTableExists(String tableName)
    {
        Failsafe.with(
                        new RetryPolicy<>()
                                .withMaxAttempts(10)
                                .withDelay(Duration.ofMillis(100)))
                .run(() -> assertTrue(schemaExists()));
        Failsafe.with(
                        new RetryPolicy<>()
                                .withMaxAttempts(10)
                                .withDelay(Duration.ofMillis(100)))
                .run(() -> assertTrue(tableExists(tableName)));
    }

    private boolean schemaExists()
    {
        return computeActual(format("SHOW SCHEMAS FROM %s LIKE '%s'", getSession().getCatalog().get(), getSession().getSchema().get())).getRowCount() == 1;
    }

    private boolean tableExists(String tableName)
    {
        return computeActual(format("SHOW TABLES LIKE '%s'", tableName.toLowerCase(ENGLISH))).getRowCount() == 1;
    }

    private void assertCount(String tableName, int count)
    {
        assertQuery(format("SELECT count(*) FROM %s", toDoubleQuoted(tableName)), format("VALUES (%s)", count));
    }

    private static Descriptor getInitialSchema()
            throws Exception
    {
        return getDescriptor("initial_schema.proto");
    }

    private static Descriptor getEvolvedSchema()
            throws Exception
    {
        return getDescriptor("evolved_schema.proto");
    }

    private static Descriptor getKeySchema()
            throws Exception
    {
        return getDescriptor("key_schema.proto");
    }

    public static Descriptor getDescriptor(String fileName)
            throws Exception
    {
        return getFileDescriptor(getProtoFile("protobuf/" + fileName)).findMessageTypeByName(DEFAULT_MESSAGE);
    }

    private static String toDoubleQuoted(String tableName)
    {
        return format("\"%s\"", tableName);
    }

    private static String toSingleQuoted(Object value)
    {
        requireNonNull(value, "value is null");
        return format("'%s'", value);
    }

    private static List<ProducerRecord<DynamicMessage, DynamicMessage>> createMessages(String topicName, int messageCount, boolean useInitialSchema, Descriptor descriptor, Descriptor keyDescriptor)
    {
        ImmutableList.Builder<ProducerRecord<DynamicMessage, DynamicMessage>> producerRecordBuilder = ImmutableList.builder();
        if (useInitialSchema) {
            for (long key = 0; key < messageCount; key++) {
                producerRecordBuilder.add(new ProducerRecord<>(topicName, createKeySchema(key, keyDescriptor), createRecordWithInitialSchema(key, descriptor)));
            }
        }
        else {
            for (long key = 0; key < messageCount; key++) {
                producerRecordBuilder.add(new ProducerRecord<>(topicName, createKeySchema(key, keyDescriptor), createRecordWithEvolvedSchema(key, descriptor)));
            }
        }
        return producerRecordBuilder.build();
    }

    private static DynamicMessage createKeySchema(long key, Descriptor descriptor)
    {
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("key"), key)
                .build();
    }

    private static DynamicMessage createRecordWithInitialSchema(long key, Descriptor descriptor)
    {
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("col_1"), format("string-%s", key))
                .setField(descriptor.findFieldByName("col_2"), multiplyExact(key, 100))
                .build();
    }

    private static DynamicMessage createRecordWithEvolvedSchema(long key, Descriptor descriptor)
    {
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("col_1"), format("string-%s", key))
                .setField(descriptor.findFieldByName("col_2"), multiplyExact(key, 100))
                .setField(descriptor.findFieldByName("col_3"), (key + 10.1D) / 10.0D)
                .build();
    }
}
