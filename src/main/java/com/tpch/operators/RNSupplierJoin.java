package com.tpch.operators;
import com.tpch.POJO.Supplier;
import com.tpch.utils.OutputBoolean;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

// left: Tuple5<Boolean, String, Integer, String, Integer>
// update, region.name, region.regionKey, nation.name, nation.nationKey

// right

//return left.f2, left.f1, left.f4, left.f3, supplier.suppKey
// Tuple6<Boolean, Integer, String, Integer, String, Integer>
//return update, region.regionKey, region.name, nation.nationKey, nation.name, supplier.suppKey
public class RNSupplierJoin extends CoProcessFunction<
        Tuple5<Boolean, String, Integer, String, Integer>,
        Supplier,
        Tuple6<Boolean, Integer, String, Integer, String, Integer>> {
    private ValueState<Tuple5<Boolean, String, Integer, String, Integer>> leftState;
    private ListState<Supplier> supplierListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        leftState = getRuntimeContext().getState(
                new ValueStateDescriptor<Tuple5<Boolean, String, Integer, String, Integer>>("leftState",
                        Types.TUPLE(Types.BOOLEAN, Types.STRING, Types.INT, Types.STRING, Types.INT))
        );
        supplierListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Supplier>("supplierState", Supplier.class)
        );
    }

    @Override
    public void processElement1(Tuple5<Boolean, String, Integer, String, Integer> left, CoProcessFunction<Tuple5<Boolean, String, Integer, String, Integer>, Supplier, Tuple6<Boolean, Integer, String, Integer, String, Integer>>.Context context, Collector<Tuple6<Boolean, Integer, String, Integer, String, Integer>> collector) throws Exception {
        Tuple5<Boolean, String, Integer, String, Integer> oldleft= leftState.value();

        if ((oldleft == null && left.f0) || (oldleft != null && !left.f0)) {
            leftState.update(left);
            for (Supplier supplier : supplierListState.get()) {
                boolean update = OutputBoolean.isUpdate(left.f0, supplier.update);
                Tuple6<Boolean, Integer, String, Integer, String, Integer> tuple = Tuple6.of(update, left.f2, left.f1, left.f4, left.f3, supplier.suppKey);
                if (OutputBoolean.canCollect(left.f0, supplier.update)) {
                    collector.collect(tuple);
                }
            }
        }
    }

    @Override
    public void processElement2(Supplier supplier, CoProcessFunction<Tuple5<Boolean, String, Integer, String, Integer>, Supplier, Tuple6<Boolean, Integer, String, Integer, String, Integer>>.Context context, Collector<Tuple6<Boolean, Integer, String, Integer, String, Integer>> collector) throws Exception {
        boolean canAddToList = supplier.update;
        if (!canAddToList) {
            int insertCount = 0;
            int deleteCount = 0;
            for (Supplier s : supplierListState.get()) {
                if (s.update) {
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
        supplierListState.add(supplier);
        Tuple5<Boolean, String, Integer, String, Integer> left = leftState.value();
        if (left != null) {
            boolean update = OutputBoolean.isUpdate(left.f0, supplier.update);
            Tuple6<Boolean, Integer, String, Integer, String, Integer>tuple = Tuple6.of(update, left.f2, left.f1, left.f4, left.f3, supplier.suppKey);
            if (OutputBoolean.canCollect(left.f0, supplier.update)) {
                collector.collect(tuple);
            }
        }
    }
}
