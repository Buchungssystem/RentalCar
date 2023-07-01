package org.car;
import com.fasterxml.jackson.annotation.JsonProperty;


public class Car {
    @JsonProperty("id")
    private int id;

    @JsonProperty("name")
    private String brand;

    @JsonProperty("price")
    private String type;

    public Car(int pId, String pBrand, String pType){
        id = pId;
        brand = pBrand;
        type = pType;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
