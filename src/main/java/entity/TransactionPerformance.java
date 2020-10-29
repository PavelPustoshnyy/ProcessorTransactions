package entity;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;


public class TransactionPerformance {
    private String ClientPin;
    private Integer StepId;
    private String EventDttm;
    private Double AmtRur;

    public TransactionPerformance() {}

    public TransactionPerformance(Transaction transaction, Integer StepId) {
            this.ClientPin = transaction.GetClientPin();
            this.StepId = StepId;
            this.EventDttm = CalcDate();
            this.AmtRur = 0.0;
    }

    public void Update(Transaction transaction) {
            this.AmtRur += transaction.GetReqAmt();
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


    @JsonAlias("client_pin")
    @JsonSetter("client_pin")
    public void setClientPin(String clientPin) {
        this.ClientPin=clientPin;
    }

    @JsonAlias("amt_rur")
    @JsonSetter("amt_rur")
    public void setReqAmt(Double reqAmt) {
        this.AmtRur=reqAmt;
    }

    @JsonAlias("step_id")
    @JsonSetter("step_id")
    public void setMerchant(Integer Merchant) {
        this.StepId=Merchant;
    }

    @JsonAlias("event_dttm")
    @JsonSetter("event_dttm")
    public void setUTime(String EventDttm) {
        this.EventDttm=EventDttm;
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

    @Override
    public String toString() {
        return "{\"client_pin\":\"" + this.ClientPin +
                "\",\"amt_rur\":" + this.AmtRur +
                ",\"step_id\":\"" + this.StepId +
                "\",\"event_dttm\":\"" + this.EventDttm + "\"}";
    }
}
