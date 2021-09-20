package ru.netology.dsw.processor;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.netology.dsw.TestUtils;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class PriceAlertsAppTest {
    private final MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    private final StringSerializer stringSerializer = new StringSerializer();
    private final StringDeserializer stringDeserializer = new StringDeserializer();

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Object> purchasesTopic;
    private TestOutputTopic<String, Object> resultTopic;
    private TestInputTopic<String, Object> productsTopic;

    @BeforeEach
    void setUp() {
        var serDeProps = Map.of(
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true",
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8090"
        );
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistry, serDeProps);
        KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, serDeProps);
        testDriver = new TopologyTestDriver(PriceAlertsApp.buildTopology(schemaRegistry, serDeProps), PriceAlertsApp.getStreamsConfig());
        purchasesTopic = testDriver.createInputTopic(PriceAlertsApp.PURCHASE_TOPIC_NAME, stringSerializer, avroSerializer);
        productsTopic = testDriver.createInputTopic(PriceAlertsApp.PRODUCT_TOPIC_NAME, stringSerializer, avroSerializer);
        resultTopic = testDriver.createOutputTopic(PriceAlertsApp.RESULT_TOPIC, stringDeserializer, avroDeserializer);
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    public void shouldAlertIfThereAreManySmallPurchasesOfAProduct() throws Exception {
        Instant twoMinutesAgo = Instant.now().minusSeconds(120);
        productsTopic.pipeInput("1", createTestProduct(300));
        for (int i = 0; i < 6; i++) {
            // эмулируем получение шести покупок с количеством 2 две минуты назад
            purchasesTopic.pipeInput(
                    // id сообщения
                    String.valueOf(i),
                    // само сообщение
                    createPurchase(1, 2),
                    // таймстемп сообщения
                    twoMinutesAgo
            );
        }
        // симулируем, что окно завершилось
        testDriver.advanceWallClockTime(Duration.ofMinutes(2));
        // итого получается куплено 12 айтемов по цене 300 - суммарно 3600
        // больше, чем требуемая для алерта сумма - 3000

        var result = resultTopic.readKeyValue();
        assertThat(result.key, is("1"));
        assertThat(
                ((GenericRecord) result.value).get("window_start"),
                // начало окна - это каждая минута в 0 секунд
                is(twoMinutesAgo.truncatedTo(ChronoUnit.MINUTES).toEpochMilli())
        );
        assertThat(((GenericRecord) result.value).get("total_sum_per_minute"), is(3600D));
    }

    @Test
    public void shouldAlertIfThereIsOneBigPurchase() throws Exception {
        Instant twoMinutesAgo = Instant.now().minusSeconds(120);
        // эмулируем получение одной большой покупки с количеством 100 две минуты назад
        productsTopic.pipeInput("1", createTestProduct(31));
        purchasesTopic.pipeInput(
                // id сообщения
                String.valueOf(123),
                // само сообщение
                createPurchase(1, 100),
                // таймстемп сообщения
                twoMinutesAgo
        );
        // симулируем, что окно завершилось
        testDriver.advanceWallClockTime(Duration.ofMinutes(2));
        // итого получается куплено 100 айтемов по цене 31 - суммарно 3100
        // больше, чем требуемая для алерта сумма - 3000

        var result = resultTopic.readKeyValue();

        assertThat(result.key, is("1"));
        assertThat(
                ((GenericRecord) result.value).get("window_start"),
                // начало окна - это каждая минута в 0 секунд
                is(twoMinutesAgo.truncatedTo(ChronoUnit.MINUTES).toEpochMilli())
        );
        assertThat(((GenericRecord) result.value).get("total_sum_per_minute"), is(3100D));
    }

    private GenericRecord createPurchase(long productId, long quantity) {
        GenericRecord purchase = new GenericData.Record(TestUtils.createPurchaseSchema());
        purchase.put("id",  new Random().nextLong());
        purchase.put("quantity", quantity);
        purchase.put("productid", productId);
        return purchase;
    }

    private GenericRecord createTestProduct(double price) {
        GenericRecord record = new GenericData.Record(TestUtils.createProductSchema());
        record.put("id", 1L);
        record.put("name", new Utf8("TV"));
        record.put("description", new Utf8("TV set"));
        record.put("price", price);
        return record;
    }
}