package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.DauService;
import com.atguigu.gmallpublisher.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class OrderServiceImpl implements OrderService {


    @Autowired
    OrderMapper  orderMapper ;

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHourMap(date);
        Map orderAmountHourMap=new HashMap();
        for (Map map : mapList) {
            orderAmountHourMap.put(map.get("CREATE_HOUR"), map.get("SUM_AMOUNT"));
        }
        return orderAmountHourMap;
    }
}
