import entity.Merchant;
import entity.Props;
import entity.Transaction;
import entity.TransactionPerformance;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import processor.TransactionProcessor;
import processor.TransactionSupplier;
import serdes.JsonPOJODeserializer;
import serdes.JsonPOJOSerializer;
import serdes.SerDeFactory;

import java.util.HashMap;
import java.util.Properties;

public class ProcessorApplication {
    public static void main(String[] args) throws InterruptedException {
        Props props = new Props();
        Properties p = props.getProperties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor_application");
        StreamsConfig streamsConfig = new StreamsConfig(p);

        Serde<String> stringSerde = Serdes.String();
        Deserializer<String> stringDeserializer = stringSerde.deserializer();
        Serializer<String> stringSerializer = stringSerde.serializer();

        Deserializer<Transaction> transactionDeserializer = new JsonPOJODeserializer<>(Transaction.class);
        Serializer<TransactionPerformance> transactionPerformanceSerializer = new JsonPOJOSerializer<>();

        Serde<Transaction> transactionSerde = SerDeFactory.getPOJOSerde(Transaction.class);
        Serde<TransactionPerformance> transactionPerformanceSerde = SerDeFactory.getPOJOSerde(TransactionPerformance.class);

        String inTopic = "first_topic";

        Topology topology = new Topology();

        String transactionsKeyStateStore = "transactions-key-state-store";
        String transactionsStateStore = "transactions-state-store22";

        String transactionSourceNodeName = "transaction-source";
        String fakeProcessorNodeName = "transaction-key-processor";
        String transactionProcessorNodeName = "transaction-processor";
        String transactionSinkNodeName = "transaction-sink";

        HashMap<Integer, Merchant> bonus = new HashMap<Integer, Merchant>();
        bonus.put(1, new Merchant("Cafe&Restaraunt", 3000));
        bonus.put(2, new Merchant("E-Commerce", 5000));
        bonus.put(3, new Merchant("Supermarkets", 3000));
        HashMap<String, Integer> bonusNum = new HashMap<String, Integer>();
        bonusNum.put("Cafe&Restaraunt", 1);
        bonusNum.put("E-Commerce", 2);
        bonusNum.put("Supermarkets", 3);

        ProcessorSupplier<String, Transaction> transactionSupplier = new TransactionSupplier(transactionsKeyStateStore);


        KeyValueBytesStoreSupplier storeKeySupplier = Stores.inMemoryKeyValueStore(transactionsKeyStateStore);
        StoreBuilder<KeyValueStore<String, Transaction>> storeKeyBuilder =
                Stores.keyValueStoreBuilder(storeKeySupplier, stringSerde, transactionSerde).withLoggingDisabled();

        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(transactionsStateStore);
        StoreBuilder<KeyValueStore<String, TransactionPerformance>> storeBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, stringSerde, transactionPerformanceSerde);

        topology.addGlobalStore(storeKeyBuilder, transactionSourceNodeName, stringDeserializer, transactionDeserializer,
                inTopic, fakeProcessorNodeName, transactionSupplier)
                .addProcessor(transactionProcessorNodeName, () -> new TransactionProcessor(transactionsStateStore, bonus, bonusNum),
                        fakeProcessorNodeName)
                .addStateStore(storeBuilder, transactionProcessorNodeName)
                .addSink(transactionSinkNodeName, "analytics", stringSerializer, transactionPerformanceSerializer, transactionProcessorNodeName);


        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig);
        System.out.println("Stock Analysis App Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(70000);
        System.out.println("Shutting down the Stock Analysis App now");
        kafkaStreams.close();
    }
}
