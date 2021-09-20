package ru.netology.dsw.processor;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class PriceAlertsApp {
    public static final String PURCHASE_TOPIC_NAME = "purchases";
    public static final String PRODUCT_TOPIC_NAME = "products";
    public static final String RESULT_TOPIC = "product_quantity_alerts-processor";
    public static final long MAX_PURCHASES_PER_MINUTE = 10L;
    public static final String AGGREGATE_STATE_STORE_NAME = "aggregate-state-store";
    public static final String PRODUCTS_STATE_STORE_NAME = "products-state-store";

    public static void main(String[] args) throws InterruptedException {
        // создаем клиент для общения со schema-registry
        var client = new CachedSchemaRegistryClient("http://localhost:8090", 16);
        var serDeProps = Map.of(
                // указываем сериализатору, что может самостояетльно регистрировать схемы
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true",
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8090"
        );

        // строим нашу топологию
        Topology topology = buildTopology(client, serDeProps);

        System.out.println(topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getStreamsConfig());
        // вызов latch.await() будет блокировать текущий поток
        // до тех пор пока latch.countDown() не вызовут 1 раз
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        try {
            kafkaStreams.start();
            // будет блокировать поток, пока из другого потока не будет вызван метод countDown()
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static Properties getStreamsConfig() {
        Properties props = new Properties();
        // имя этого приложения для кафки
        // приложения с одинаковым именем объединятся в ConsumerGroup и распределят обработку партиций между собой
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ProductJoinerProcessorAPI");
        // адреса брокеров нашей кафки (у нас он 1)
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // если вы захотите обработать записи заново, не забудьте удалить папку со стейтами
        // а лучше воспользуйтесь методом kafkaStreams.cleanUp()
        props.put(StreamsConfig.STATE_DIR_CONFIG, "states");
        return props;
    }

    public static Topology buildTopology(SchemaRegistryClient client, Map<String, String> serDeConfig) {
        // Создаем класс для сериализации и десериализации наших сообщений
        var avroSerde = new GenericAvroSerde(client);
        avroSerde.configure(serDeConfig, false);

        Topology topology = new Topology();

        // Получаем из кафки поток сообщений из топика покупок
        topology.addSource(
                "purchases-source",
                new StringDeserializer(), new KafkaAvroDeserializer(client, serDeConfig),
                PURCHASE_TOPIC_NAME
        );

        // Добавляем ноду для обработки наших сообщений
        topology.addProcessor(
                "alerts-transformer",  // указываем имя процессора
                PurchaseQuantityAlertTrasformer::new, // указываем, как создать класс, который будет обрабатывать сообщения
                "purchases-source" // указываем имя предыдущего процессора
        );

        var aggregateStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(
                        AGGREGATE_STATE_STORE_NAME // обязательно указываемя имя стора - оно нам пригодится в нашем трансформере
                ),
                Serdes.ByteArray(), new Serdes.DoubleSerde()
        );
        // добавляем стейт стор для агрегации в топологию
        topology.addStateStore(aggregateStoreSupplier, "alerts-transformer");
        var productsStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(
                        PRODUCTS_STATE_STORE_NAME // обязательно указываемя имя стора - оно нам пригодится в нашем трансформере
                ),
                new Serdes.StringSerde(), avroSerde
        ).withLoggingDisabled();
        // добавляем глобал стор (global - значит, что джоиним все партиции в каждый инстанс приложения)
        // для получение продукта по id
        topology.addGlobalStore(
                productsStoreSupplier,
                "products-source",
                new StringDeserializer(),
                new KafkaAvroDeserializer(client),
                PRODUCT_TOPIC_NAME,
                "products-store",
                new StateUpdateSupplier(PRODUCTS_STATE_STORE_NAME)
        );

        // Добавляем указание, куда писать наши алерты
        topology.addSink(
                "sink", // указываем имя процессора
                RESULT_TOPIC, // указываем топик, в который отправить сообщения
                new StringSerializer(), new KafkaAvroSerializer(client, serDeConfig),
                "alerts-transformer" // указываем имя предыдущего процессора
        );

        return topology;
    }
}
