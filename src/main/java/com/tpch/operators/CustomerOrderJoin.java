package com.tpch.operators;
import com.tpch.POJO.Customer;
import com.tpch.POJO.Orders;
import com.tpch.utils.OutputBoolean;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import java.sql.Date;

public class CustomerOrderJoin extends CoProcessFunction<Customer, Orders, Tuple5<Boolean, Integer, Integer, Integer, Date>> {
    private ValueState<Customer> customerState;

    private ListState<Orders> orderListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        customerState = getRuntimeContext().getState(
                new ValueStateDescriptor<Customer>("customerState", Customer.class)
        );
        orderListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Orders>("orderState", Orders.class)
        );
    }

    @Override
    public void processElement1(Customer customer, CoProcessFunction<Customer, Orders, Tuple5<Boolean, Integer, Integer, Integer, Date>>.Context context, Collector<Tuple5<Boolean, Integer, Integer, Integer, Date>> collector) throws Exception {
        Customer oldCustomer = customerState.value();

        if ((oldCustomer == null && customer.update) || (oldCustomer != null && !customer.update)) {
            customerState.update(customer);
            for (Orders order: orderListState.get()) {
                boolean update = OutputBoolean.isUpdate(customer.update, order.update);
                Tuple5<Boolean, Integer, Integer, Integer, Date> tuple = Tuple5.of(update, customer.custKey, customer.nationKey, order.orderKey, order.orderDate);
                if (OutputBoolean.canCollect(customer.update, order.update)) {
                    collector.collect(tuple);
                }
            }
        }
    }


    @Override
    public void processElement2(Orders orders, CoProcessFunction<Customer, Orders, Tuple5<Boolean, Integer, Integer, Integer, Date>>.Context context, Collector<Tuple5<Boolean, Integer, Integer, Integer, Date>> collector) throws Exception {
        boolean canAddToList = orders.update;
        if (!canAddToList) {
            int insertCount = 0;
            int deleteCount = 0;
            for (Orders o : orderListState.get()) {
                if (o.update) {
                    insertCount += 1;
                } else {
                    deleteCount += 1;
                }
            }
            if (insertCount > deleteCount) {
                canAddToList = true;
            }
        }
        if (!canAddToList) {
            return;
        }
        orderListState.add(orders);
        Customer customer = customerState.value();
        if (customer != null) {
            boolean update = OutputBoolean.isUpdate(customer.update, orders.update);
            Tuple5<Boolean, Integer, Integer, Integer, Date> tuple = Tuple5.of(update, customer.custKey, customer.nationKey, orders.orderKey, orders.orderDate);
            if (OutputBoolean.canCollect(customer.update, orders.update)) {
                collector.collect(tuple);
            }
        }
    }
}
