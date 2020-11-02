package processor;

import entity.Transaction;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class TransactionKeyProcessor extends AbstractProcessor<String, Transaction> {
    private KeyValueStore<String, Transaction> keyValueStore;
    private String stateStoreName;
    public TransactionKeyProcessor(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext processorContext) {
        super.init(processorContext);
        keyValueStore = (KeyValueStore) context().getStateStore(stateStoreName);
    }

    @Override
    public void process(String ClientPin, Transaction transaction) {
        keyValueStore.put(transaction.GetClientPin(),transaction);
        context().forward(transaction.GetClientPin(),transaction);

    }
}
