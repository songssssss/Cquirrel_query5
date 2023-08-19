package com.tpch.operators;
import com.tpch.utils.OutputBoolean;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Date;

// left: Tuple6<Boolean, Integer, String, Integer, String, Integer>
// left: update, region.regionKey, region.name, nation.nationKey, nation.name, supplier.suppKey

// right: update, customer.custKey, customer.nationKey, orders.orderKey, orders.orderDate, lineitem.extendedPrice, lineitem.discount, lineitem.suppKey
// right: Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>

// return: update, region.name, nation.name, orders.orderDate, lineitem.extendedPrice, lineitem.discount
// Tuple6<Boolean, String, String, Date, Double, Double>
// update, left.f2, left.f4, right.f4, right.f5, right.f6
// key by nationKey: t1 -> t1.f3, t2 -> t2.f2, supplierKey: t1 -> t1.f5, t2 -> t2.f7

// return: update, region.name, nation.name, nation.nationKey, customer.nationKey, supplier.suppKey, lineitem.suppKey, orders.orderDate, lineitem.extendedPrice, lineitem.discount
// Tuple10<Boolean, String, String, Integer, Integer,Integer, Integer, Date, Double, Double>
// update, left.f2, left.f4, left.f3, right.f2, left.f5, right.f7, right.f4, right.f5, right.f6
// key by supplierKey: t1 -> t1.f5, t2 -> t2.f7
// nationKey: t1 -> t1.f3, t2 -> t2.f2,
public class RNSCOLJoin extends CoProcessFunction<
        Tuple6<Boolean, Integer, String, Integer, String, Integer>,
        Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>,
        Tuple10<Boolean, String, String, Integer, Integer,Integer, Integer, Date, Double, Double>> {

    private ListState<Tuple6<Boolean, Integer, String, Integer, String, Integer>> leftListState;
    private ListState<Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>> rightListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        leftListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Tuple6<Boolean, Integer, String, Integer, String, Integer>>("leftList",
                        Types.TUPLE(Types.BOOLEAN, Types.INT, Types.STRING, Types.INT, Types.STRING, Types.INT))
        );

        rightListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>>("rightList",
                        Types.TUPLE(Types.BOOLEAN, Types.INT, Types.INT, Types.INT,Types.LOCAL_DATE, Types.DOUBLE, Types.DOUBLE, Types.INT))
        );
    }

    @Override
    public void processElement1(Tuple6<Boolean, Integer, String, Integer, String, Integer> left, CoProcessFunction<Tuple6<Boolean, Integer, String, Integer, String, Integer>, Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>, Tuple10<Boolean, String, String, Integer, Integer,Integer, Integer, Date, Double, Double>>.Context context, Collector<Tuple10<Boolean, String, String, Integer, Integer,Integer, Integer, Date, Double, Double>> collector) throws Exception {
        leftListState.add(left);
        for (Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer> right : rightListState.get()) {
            boolean update = OutputBoolean.isUpdate(left.f0, right.f0);
            if (OutputBoolean.canCollect(left.f0, right.f0)) {
                Tuple10<Boolean, String, String, Integer, Integer,Integer, Integer, Date, Double, Double> tuple = Tuple10.of(update, left.f2, left.f4, left.f3, right.f2, left.f5, right.f7, right.f4, right.f5, right.f6);
                collector.collect(tuple);
            }
        }
    }

    @Override
    public void processElement2(Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer> right, CoProcessFunction<Tuple6<Boolean, Integer, String, Integer, String, Integer>, Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>, Tuple10<Boolean, String, String, Integer, Integer,Integer, Integer, Date, Double, Double>>.Context context, Collector<Tuple10<Boolean, String, String, Integer, Integer,Integer, Integer, Date, Double, Double>> collector) throws Exception {
        rightListState.add(right);
        for (Tuple6<Boolean, Integer, String, Integer, String, Integer> left : leftListState.get()) {
            boolean update = OutputBoolean.isUpdate(left.f0, right.f0);
            if (OutputBoolean.canCollect(left.f0, right.f0)) {
                Tuple10<Boolean, String, String, Integer, Integer,Integer, Integer, Date, Double, Double> tuple = Tuple10.of(update, left.f2, left.f4, left.f3, right.f2, left.f5, right.f7, right.f4, right.f5, right.f6);
                collector.collect(tuple);
            }
        }
    }
}

