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

    public TransactionProcessor (String stateStoreName, HashMap<Integer, Merchant> bonus, HashMap<String, Integer> bonusNum) {
        this.stateStoreName = stateStoreName;
        this.bonus = bonus;
        this.bonusNum=bonusNum;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext processorContext) {
        super.init(processorContext);
        keyValueStore = (KeyValueStore) context().getStateStore(stateStoreName);
        //TransactionPunctuator punctuator = new TransactionPunctuator(bonus,
            //    context(),
         //       keyValueStore);

       // context().schedule(36000, PunctuationType.WALL_CLOCK_TIME, punctuator);
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
        System.out.println("Пришла транзакция");
        System.out.println(transaction.toString());

        TransactionPerformance transactionPerformance =
                keyValueStore.get(transaction.GetClientPin()+this.bonusNum.get(transaction.GetMerchant()).toString());
        System.out.println("Ключ: " + transaction.GetClientPin() + this.bonusNum.get(transaction.GetMerchant()).toString());

        if (transactionPerformance == null) {
            System.out.println("Ключ не нашелся");
            FirstPut(transactionPerformance, transaction);
        } else {
            System.out.println("Ключ нашелся");
            NextPut(transactionPerformance,transaction);
        }
        System.out.println("");
//

        //// перестраховка - удаление лишних строк с прошлого запуска прогарммы
        ////if (transactionPerformance != null && !this.bonus.containsKey(transactionPerformance.GetStepId())) {
        ////    System.out.println("delete");
        ////    keyValueStore.delete(transaction.GetClientPin());
        ////    transactionPerformance = null;
        ////}
        //System.out.println("Пришла транзакция");
        //System.out.println(transaction.toString());
        ////транзакция по этому пину пришла в перый раз
        //if (transactionPerformance == null) {
        //    System.out.println("транзакция по этому пину пришла в перый раз");
//
        //    // совпадение типа транзакции с первым типом
        //    if (this.bonus.get(1).GetType().equals(transaction.GetMerchant())) {
        //        System.out.println("совпадение типа транзакции с первым типом");
        //        transactionPerformance = new TransactionPerformance(transaction, 1);
        //        transactionPerformance.Update(transaction);
        //        System.out.println(transactionPerformance.toString());
        //        keyValueStore.put(transactionPerformance.GetClientPin()+"1", transactionPerformance);
        //    }
//
        //    // при несовпадении ничего не происходит
        //    //System.out.println("при несовпадении ничего не происходит");
        //// транзакция по этому пину уже была
        //} else {
        //    System.out.println("транзакция по этому пину уже была");
        //    System.out.println(transactionPerformance.toString());
//
        //    // случай достижения лимита
        //    if (this.bonus.containsKey(transactionPerformance.GetStepId())
        //            && transactionPerformance.GetReqAmt() >= bonus.get(transactionPerformance.GetStepId()).GetCost()) {
        //        System.out.println("случай достижения лимита");
        //        // проверка, является ли текущий тип не последним
        //        if (this.bonus.size() >= transactionPerformance.GetStepId() + 1) {
        //            System.out.println("тип не последний");
        //            // является ли тип новой транзакция следующим типом
        //            if (this.bonus.get(transactionPerformance.GetStepId()+1).GetType().equals(transaction.GetMerchant())) {
        //                System.out.println("тип новой транзакция - следующий тип");
        //                System.out.println(transactionPerformance.toString());
        //                transactionPerformance = new TransactionPerformance(transaction, transactionPerformance.GetStepId() + 1);
        //                transactionPerformance.Update(transaction);
        //                System.out.println(transactionPerformance.toString());
        //                keyValueStore.put(transactionPerformance.GetClientPin()+transactionPerformance.GetStepId().toString(), transactionPerformance);
        //            }
        //        }
//
        //        // если тип последний, ничего не происходит
        //        //System.out.println("тип последний");
        //    // если достижения лимита по текущему типу нет:
        //    } else {
        //        System.out.println("достижения лимита по текущему типу нет");
        //        // совпадение типа - просто апдейт
        //        if (this.bonus.get(transactionPerformance.GetStepId()).GetType().equals(transaction.GetMerchant())) {
        //            System.out.println("совпадение типа - просто апдейт");
        //            System.out.println(transactionPerformance.toString());
        //            transactionPerformance.Update(transaction);
        //            System.out.println(transactionPerformance.toString());
        //            keyValueStore.put(transactionPerformance.GetClientPin()+transactionPerformance.GetStepId().toString(), transactionPerformance);
        //            if (this.bonus.containsKey(transactionPerformance.GetStepId())
        //                    && transactionPerformance.GetReqAmt() >= bonus.get(transactionPerformance.GetStepId()).GetCost()) {
        //                keyValueStore.put(transactionPerformance.GetClientPin()+(transactionPerformance.GetStepId()+1).toString(), transactionPerformance);
        //            }
        //        }
//
        //        // в случае несовпадения - ничего не происходит
        //        //System.out.println("несовпадение типа транзакции");
        //    }
//
        //}
        //System.out.println("");
    }
}
