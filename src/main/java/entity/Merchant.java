package entity;

public class Merchant {
    private String type;
    private Integer cost;

    public Merchant(String type, Integer cost) {
        this.type = type;
        this.cost = cost;
    }
    public String GetType() {
        return this.type;
    }
    public Integer GetCost() {
        return this.cost;
    }

}
