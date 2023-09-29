package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.topology.OrdersTopology.*;

@Service
@Slf4j
public class OrderService {

    private OrderStoreService orderStoreService;

    public OrderService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrderCountPerStore> getOrderCount(String orderType) {

       var orderCountStore =  getOrderStore(orderType);

       var orders = orderCountStore.all();

       var spliterator = Spliterators.spliteratorUnknownSize(orders,0);

        return StreamSupport.stream(spliterator,false)
                .map(keyValue -> new OrderCountPerStore(keyValue.key,keyValue.value))
                .collect(Collectors.toList());
    }

    private ReadOnlyKeyValueStore<String,Long> getOrderStore(String orderType) {

      return   switch (orderType){
            case GENERAL_ORDERS -> orderStoreService.orderCountStore(GENERAL_ORDERS_COUNT);
            case RESTAURANT_ORDERS -> orderStoreService.orderCountStore(RESTAURANT_ORDERS_COUNT);
            default -> throw new IllegalStateException("Not an valid option");
        };

    }

    public OrderCountPerStore getOrderCountByLocationId(String orderType, String locationId) {

        var orderCountStore =  getOrderStore(orderType);

        var orders = orderCountStore.get(locationId);

        if (orderCountStore != null){
            return new OrderCountPerStore(locationId,orders);
        }
        return null;
    }

    public List<AllOrdersCountPerStore> getAllOrdersCount() {

        BiFunction<OrderCountPerStore, OrderType,AllOrdersCountPerStore> mapper = (orderCountStore,orderType) -> new
                AllOrdersCountPerStore(orderCountStore.locationId(),orderCountStore.orderCount(),orderType);


        var generalOrderCount = getOrderCount(GENERAL_ORDERS)
                .stream()
                .map(orderCountPerStore -> mapper.apply(orderCountPerStore,OrderType.GENERAL))
                .collect(Collectors.toList());

        var restarauntOrderCount = getOrderCount(RESTAURANT_ORDERS)
                .stream()
                .map(orderCountPerStore -> mapper.apply(orderCountPerStore,OrderType.RESTAURANT))
                .collect(Collectors.toList());

        return Stream.of(generalOrderCount,restarauntOrderCount)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<OrderRevenueDTO> revenueByOrderType(String orderType) {
          var revenueStoreByType =  getRevenueStore(orderType);

        var revenueStore = revenueStoreByType.all();

        var spliterator = Spliterators.spliteratorUnknownSize(revenueStore,0);

        return StreamSupport.stream(spliterator,false)
                .map(keyValue -> new OrderRevenueDTO(keyValue.key,mapOrder(orderType),keyValue.value))
                .collect(Collectors.toList());
    }

    private OrderType mapOrder(String orderType) {
        return   switch (orderType){
            case GENERAL_ORDERS -> OrderType.GENERAL;
            case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> throw new IllegalStateException("Not an valid option");
        };
    }

    private ReadOnlyKeyValueStore<String, TotalRevenue> getRevenueStore(String orderType) {

        return   switch (orderType){
            case GENERAL_ORDERS -> orderStoreService.orderRevenueStore(GENERAL_ORDERS_REVENUE);
            case RESTAURANT_ORDERS -> orderStoreService.orderRevenueStore(RESTAURANT_ORDERS_REVENUE);
            default -> throw new IllegalStateException("Not an valid option");
        };

    }
}
