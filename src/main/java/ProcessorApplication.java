import entity.Merchant;
import entity.Props;
import entity.Transaction;
import entity.TransactionPerformance;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.UsePreviousTimeOnInvalidTimestamp;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import processor.TransactionProcessor;
import serdes.JsonPOJODeserializer;
import serdes.JsonPOJOSerializer;
import serdes.SerDeFactory;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.LATEST;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ProcessorApplication {
    public static void main(String[] args) throws InterruptedException {
        Props props = new Props();
        Properties p = props.getProperties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor_application22313");
        StreamsConfig streamsConfig = new StreamsConfig(p);

        Serde<String> stringSerde = Serdes.String();
        Deserializer<String> stringDeserializer = stringSerde.deserializer();
        Serializer<String> stringSerializer = stringSerde.serializer();

        Deserializer<Transaction> transactionDeserializer = new JsonPOJODeserializer<>(Transaction.class);
        Serializer<TransactionPerformance> transactionPerformanceSerializer = new JsonPOJOSerializer<>();

        Serde<TransactionPerformance> transactionPerformanceSerde = SerDeFactory.getPOJOSerde(TransactionPerformance.class);

        Topology topology = new Topology();
        String transactionsStateStore = "transactions-state-store251822";

        String transactionSourceNodeName = "transaction-source";
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

        Map<String, String> changeLogConfigs = new HashMap<>();
        changeLogConfigs.put("retention.ms","70000" );
        changeLogConfigs.put("cleanup.policy", "compact,delete");

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(transactionsStateStore)
                ;
        StoreBuilder<KeyValueStore<String, TransactionPerformance>> storeBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), transactionPerformanceSerde).withLoggingEnabled(changeLogConfigs);


        topology.addSource(transactionSourceNodeName, stringDeserializer, transactionDeserializer,"first_topic")
                .addProcessor(transactionProcessorNodeName, () -> new TransactionProcessor(transactionsStateStore, bonus,bonusNum), transactionSourceNodeName)
                .addStateStore(storeBuilder,transactionProcessorNodeName)
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
