package com.tpch.operators;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.sql.Date;

/**
 * Project name: CquirrelDemo
 * Class name: LineItemSubProcess
 * Description: TODO
 * Create time: 2023/2/11 19:03
 * Creator: ellilachen
 */
// return: update, region.name, nation.name, nation.nationKey, customer.nationKey, supplier.suppKey, lineitem.suppKey, orders.orderDate, lineitem.extendedPrice, lineitem.discount
// Tuple10<Boolean, String, String, Integer, Integer,Integer, Integer, Date, Double, Double>
// update, left.f2, left.f4, left.f3, right.f2, left.f5, right.f7, right.f4, right.f5, right.f6

// input: Tuple6<Boolean, String, String, Date, Double, Double>
// update, region.name, nation.name, orders.orderDate, lineitem.extendedPrice, lineitem.discount

// output: Tuple3<Boolean, String, Double>
// update, nation.name, double
public class CalculateNationRevenue extends KeyedProcessFunction<String, Tuple10<Boolean, String, String, Integer, Integer,Integer, Integer, Date, Double, Double>, Tuple2<String, Double>> {
    private transient ValueState<Double> revenueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        revenueState = getRuntimeContext().getState(
                new ValueStateDescriptor<Double>("revenueState", Types.DOUBLE)
        );
    }

    @Override
    public void processElement(Tuple10<Boolean, String, String, Integer, Integer,Integer, Integer, Date, Double, Double> tuple, KeyedProcessFunction<String, Tuple10<Boolean, String, String, Integer, Integer,Integer, Integer, Date, Double, Double>, Tuple2<String, Double>>.Context context, Collector<Tuple2<String, Double>> collector) throws Exception {
        Double prevRevenue = revenueState.value();
        double revenue = prevRevenue != null ? prevRevenue : 0;

        // Calculate the revenue for the line item
        double lineItemRevenue = tuple.f8 * (1 - tuple.f9);

        if (tuple.f0) {
            revenue += lineItemRevenue;
            revenueState.update(revenue);
        } else {
            revenue -= lineItemRevenue;
            revenueState.update(revenue);
        }
        collector.collect(Tuple2.of(tuple.f2, revenue));
    }
}
