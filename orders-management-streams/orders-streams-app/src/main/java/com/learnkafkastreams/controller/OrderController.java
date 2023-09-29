package com.learnkafkastreams.controller;


import com.learnkafkastreams.domain.OrderCountPerStore;
import com.learnkafkastreams.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/orders")
public class OrderController {


    private OrderService orderService;

    @Autowired
    public OrderController(OrderService orderService){
        this.orderService = orderService;
    }

    @GetMapping("/count/{order_type}")
    public ResponseEntity<?> orderCount(@PathVariable("order_type")
                                                   String orderType,
                                     @RequestParam(value = "locationId",required = false) String locationId){

        if(StringUtils.hasLength(locationId)){
            return ResponseEntity.ok(orderService.getOrderCountByLocationId(orderType,locationId));
        }
      return   ResponseEntity.ok(orderService.getOrderCount(orderType));
    }

    @GetMapping("/count")
    public ResponseEntity<?> allOrdersCount(){
        return ResponseEntity.ok(orderService.getAllOrdersCount());
    }

    @GetMapping("/revenue/{order_type}")
    public ResponseEntity<?> orderCount(@PathVariable("order_type")
                                        String orderType){

//        if(StringUtils.hasLength(locationId)){
//            return ResponseEntity.ok(orderService.getOrderCountByLocationId(orderType,locationId));
//        }
        return   ResponseEntity.ok(orderService.revenueByOrderType(orderType));
    }
}
