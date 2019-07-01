/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datawhisperers.jsonexample;

import java.math.BigDecimal;

/**
 *
 * @author steveo
 */
public class Car {
    
    private String make;
    private String model;
    private BigDecimal cost;
    private String raw;

    public String getMake() {
        return make;
    }

    public void setMake(String make) {
        this.make = make;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public BigDecimal getCost() {
        return cost;
    }

    public void setCost(BigDecimal cost) {
        this.cost = cost;
    }

    public String getRaw() {
        return raw;
    }

    public void setRaw(String raw) {
        this.raw = raw;
    }

    @Override
    public String toString() {
        return "Car{" + "make=" + make + ", model=" + model + ", cost=" + cost + ", raw=" + raw + '}';
    }

   
    
    
}
