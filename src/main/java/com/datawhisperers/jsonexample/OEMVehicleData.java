/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datawhisperers.jsonexample;

/**
 *
 * @author steveo
 */
public class OEMVehicleData {
    
    
    private String id;
    private Integer timestampEpoch;
    private Position position;
    private double speed;
    private int direction;
    private String vehicleType;
    private String accuracy;
    private String activityType;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getTimestampEpoch() {
        return timestampEpoch;
    }

    public void setTimestampEpoch(Integer timestampEpoch) {
        this.timestampEpoch = timestampEpoch;
    }

    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public int getDirection() {
        return direction;
    }

    public void setDirection(int direction) {
        this.direction = direction;
    }

    public String getVehicleType() {
        return vehicleType;
    }

    public void setVehicleType(String vehicleType) {
        this.vehicleType = vehicleType;
    }

    public String getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(String accuracy) {
        this.accuracy = accuracy;
    }

    public String getActivityType() {
        return activityType;
    }

    public void setActivityType(String activityType) {
        this.activityType = activityType;
    }

    @Override
    public String toString() {
        return "OEMVehicleData{" + "id=" + id + ", timestampEpoch=" + timestampEpoch + ", position=" + position + ", speed=" + speed + ", direction=" + direction + ", vehicleType=" + vehicleType + ", accuracy=" + accuracy + ", activityType=" + activityType + '}';
    }
    
    
}
