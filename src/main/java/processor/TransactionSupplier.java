package processor;

import entity.Transaction;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class TransactionSupplier implements ProcessorSupplier {
    private String stateStoreName;
    public TransactionSupplier (String stateStoreName) {
        this.stateStoreName=stateStoreName;
    }
    @Override
    public Processor<String, Transaction> get() {
        return new TransactionKeyProcessor(this.stateStoreName);
    }
}
