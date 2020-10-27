package processor;

import entity.Merchant;
import entity.Transaction;
import entity.TransactionPerformance;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import punctuator.TransactionPunctuator;

import java.util.HashMap;

public class TransactionProcessor extends AbstractProcessor<String, Transaction> {
    private KeyValueStore<String, TransactionPerformance> keyValueStore;
    private String stateStoreName;
    HashMap<Integer, Merchant> bonus;

    public TransactionProcessor (String stateStoreName, HashMap<Integer, Merchant> bonus) {
        this.stateStoreName = stateStoreName;
        this.bonus = bonus;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext processorContext) {
        super.init(processorContext);
        keyValueStore = (KeyValueStore) context().getStateStore(stateStoreName);
        TransactionPunctuator punctuator = new TransactionPunctuator(bonus,
                context(),
                keyValueStore);

        context().schedule(10000, PunctuationType.WALL_CLOCK_TIME, punctuator);
    }

    @Override
    public void process(String ClientPin, Transaction transaction) {
        if (ClientPin != null) {
            TransactionPerformance transactionPerformance = keyValueStore.get(ClientPin);

            if (transactionPerformance == null) {
                transactionPerformance = new TransactionPerformance(transaction.GetClientPin(),bonus);
            }

            transactionPerformance.Update(transaction.GetReqAmt());

            keyValueStore.put(ClientPin, transactionPerformance);
        }
    }
}
