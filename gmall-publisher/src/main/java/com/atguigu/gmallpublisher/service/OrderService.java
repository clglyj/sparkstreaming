package com.atguigu.gmallpublisher.service;


import java.util.Map;

public interface OrderService {

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);

}
