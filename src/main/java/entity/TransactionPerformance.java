package entity;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonGetter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class TransactionPerformance {
    private String ClientPin;
    private Integer StepId;
    private String EventDttm;
    private Double AmtRur;
    HashMap<Integer,Merchant> bonus = new HashMap<Integer, Merchant>();

    public TransactionPerformance (String ClientPin, HashMap<Integer,Merchant> bonus){
        this.ClientPin = ClientPin;
        this.StepId = 1;
        this.EventDttm = CalcDate();
        this.AmtRur = 0.0;
        this.bonus = bonus;
    }
    public TransactionPerformance (String ClientPin){
        this.ClientPin=ClientPin;
        this.StepId = 1;
        this.EventDttm = CalcDate();
        this.AmtRur = 0.00;
        HashMap<Integer, Merchant> bonus = new HashMap<Integer, Merchant>();
        bonus.put(1, new Merchant("Cafe&Restaraunt", 3000));
        bonus.put(2, new Merchant("E-Commerce", 5000));
        bonus.put(3, new Merchant("Supermarkets", 3000));
    }


    public void Update(Double AmtRur) {
        if (this.bonus.containsKey(this.StepId) & this.AmtRur >= bonus.get(this.StepId).GetCost()) {
            this.AmtRur = 0.0;
            this.StepId += 1;
        }
        this.AmtRur += AmtRur;
        this.EventDttm = CalcDate();
    }

    private String CalcDate() {
        SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date date = new Date(System.currentTimeMillis());
        return formatter.format(date);
    }
    public Integer GetStepId() {
        return this.StepId;
    }

    public HashMap<Integer,Merchant> GetBonus() {
        return this.bonus;
    }

    @JsonAlias("client_pin")
    @JsonGetter("client_pin")
    public String GetClientPin() {
        return this.ClientPin;
    }

    @JsonAlias("amt_rur")
    @JsonGetter("amt_rur")
    public Double GetReqAmt() {
        return this.AmtRur;
    }

    @JsonAlias("step_id")
    @JsonGetter("step_id")
    public String GetMerchant() {
        return this.StepId.toString();
    }

    @JsonAlias("event_dttm")
    @JsonGetter("event_dttm")
    public String GetUTime() {
        return this.EventDttm;
    }


}
