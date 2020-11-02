package processor;

import entity.Merchant;
import entity.Transaction;
import entity.TransactionPerformance;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;

public class TransactionProcessor extends AbstractProcessor<String, Transaction> {
    private KeyValueStore<String, TransactionPerformance> keyValueStore;
    private String stateStoreName;
    HashMap<Integer, Merchant> bonus;
    HashMap<String, Integer> bonusNum;
    Integer counter = 0;

    public TransactionProcessor (String stateStoreName, HashMap<Integer, Merchant> bonus, HashMap<String, Integer> bonusNum) {
        this.stateStoreName = stateStoreName;
        this.bonus = bonus;
        this.bonusNum=bonusNum;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext processorContext) {
        super.init(processorContext);
        System.out.println("init");
        System.out.println(context());
        System.out.println(this.stateStoreName);

        System.out.println(context().getStateStore(this.stateStoreName));
        System.out.println((KeyValueStore) context().getStateStore(stateStoreName));
        this.keyValueStore = (KeyValueStore) context().getStateStore(stateStoreName);
    }

    private void Put(TransactionPerformance transactionPerformance) {
        keyValueStore.put(transactionPerformance.GetClientPin() + transactionPerformance.GetStepId().toString(), transactionPerformance);
        System.out.println("Добавлена транзакция с ключом " + transactionPerformance.GetClientPin() + transactionPerformance.GetStepId().toString());
    }

    private void Update (TransactionPerformance transactionPerformance, Transaction transaction) {
        if (transactionPerformance.GetReqAmt() >= bonus.get(transactionPerformance.GetStepId()).GetCost()
                && this.bonus.containsKey(transactionPerformance.GetStepId())) {
            System.out.println("Произошло достижение лимита");
            context().forward(transactionPerformance.GetClientPin()+transactionPerformance.GetStepId().toString(),
                    transactionPerformance);
            if (transactionPerformance.GetReqAmt() >= bonus.get(transactionPerformance.GetStepId()).GetCost()
                    && this.bonus.containsKey(transactionPerformance.GetStepId()+1)) {
                transactionPerformance = new TransactionPerformance(transaction, transactionPerformance.GetStepId() + 1);
            }
            System.out.println(transactionPerformance.toString());
            Put(transactionPerformance);
        }
    }

    private void FirstPut(TransactionPerformance transactionPerformance, Transaction transaction) {
        if (this.bonus.get(1).GetType().equals(transaction.GetMerchant())) {
            System.out.println("Тип транзакции совпал с первым типом");
            transactionPerformance = new TransactionPerformance(transaction, 1);
            System.out.println(transactionPerformance.toString());
            transactionPerformance.Update(transaction);
            Put(transactionPerformance);

            Update(transactionPerformance, transaction);
        }
    }

    private void NextPut(TransactionPerformance transactionPerformance, Transaction transaction) {
        if(!(transactionPerformance.GetReqAmt() >= bonus.get(transactionPerformance.GetStepId()).GetCost())) {
            System.out.println("Для новой транзакции лимит не превышен");
            if (this.bonus.get(transactionPerformance.GetStepId()).GetType().equals(transaction.GetMerchant())) {
                System.out.println("Тип транзакции совпал с нужным типом");
                transactionPerformance.Update(transaction);
                System.out.println(transactionPerformance.toString());
                Put(transactionPerformance);

                Update(transactionPerformance, transaction);
            }
        }
    }



    @Override
    public void process(String ClientPin, Transaction transaction) {
        this.counter++;
        System.out.println(this.counter);
        System.out.println("Пришла транзакция");
        System.out.println(transaction.toString());

        System.out.println(this.bonusNum.get(transaction.GetMerchant()).toString());
        System.out.println(transaction.GetClientPin());
        System.out.println(this.keyValueStore==null);
        System.out.println(this.keyValueStore.get(transaction.GetClientPin()));

        TransactionPerformance transactionPerformance =
                this.keyValueStore.get(transaction.GetClientPin()+this.bonusNum.get(transaction.GetMerchant()).toString());
        System.out.println("Ключ: " + transaction.GetClientPin() + this.bonusNum.get(transaction.GetMerchant()).toString());


        if (transactionPerformance == null) {
            System.out.println("Ключ не нашелся");
            FirstPut(transactionPerformance, transaction);
        } else {
            System.out.println("Ключ нашелся");
            NextPut(transactionPerformance,transaction);
        }
        System.out.println("");
    }
}
