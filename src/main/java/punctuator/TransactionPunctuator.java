package punctuator;

import entity.Merchant;
import entity.TransactionPerformance;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;

public class TransactionPunctuator implements Punctuator {
    private HashMap<Integer, Merchant> bonus;
    private ProcessorContext context;
    private KeyValueStore<String, TransactionPerformance> keyValueStore;

    public TransactionPunctuator(HashMap<Integer, Merchant> bonus,
                                 ProcessorContext context,
                                 KeyValueStore<String, TransactionPerformance> keyValueStore) {

        this.bonus = bonus;
        this.context = context;
        this.keyValueStore = keyValueStore;
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, TransactionPerformance> performanceIterator = keyValueStore.all();

        while (performanceIterator.hasNext()) {
            KeyValue<String, TransactionPerformance> keyValue = performanceIterator.next();
            String key = keyValue.key;
            TransactionPerformance stockPerformance = keyValue.value;

            if (stockPerformance != null) {
                if (stockPerformance.GetReqAmt() >= stockPerformance.GetBonus().get(stockPerformance.GetStepId()).GetCost()) {
                    context.forward(key, stockPerformance);
                }
            }
        }
    }
}
