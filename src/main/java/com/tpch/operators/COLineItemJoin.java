package com.tpch.operators;
import com.tpch.POJO.LineItem;
import com.tpch.utils.OutputBoolean;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import java.sql.Date;



// left: Tuple5<Boolean, Integer, Integer, Integer, Date>
// update, customer.custKey, customer.nationKey, orders.orderKey, orders.orderDate

// right: update, lineitem.extendedPrice, lineitem.discount, lineitem.orderKey, lineitem.suppKey

// return:  update, customer.custKey, customer.nationKey, orders.orderKey, orders.orderDate, lineitem.extendedPrice, lineitem.discount, lineitem.suppKey
// update, left.f1, left.f2, left.f3, left.f4, lineitem.extendedPrice, lineitem.discount, lineitem.suppKey
// Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>
public class COLineItemJoin extends CoProcessFunction<
        Tuple5<Boolean, Integer, Integer, Integer, Date>,
        LineItem,
        Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>> {

    private ValueState<Tuple5<Boolean, Integer, Integer, Integer, Date>> leftState;
    private ListState<LineItem> lineItemListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        leftState = getRuntimeContext().getState(
                new ValueStateDescriptor<Tuple5<Boolean, Integer, Integer, Integer, Date>>("leftState",
                        Types.TUPLE(Types.BOOLEAN, Types.INT, Types.INT, Types.INT, Types.LOCAL_DATE))
        );
        lineItemListState = getRuntimeContext().getListState(
                new ListStateDescriptor<LineItem>("lineItemState", LineItem.class)
        );
    }

    @Override
    public void processElement1(Tuple5<Boolean, Integer, Integer, Integer, Date> left, CoProcessFunction<Tuple5<Boolean, Integer, Integer, Integer, Date>, LineItem, Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>>.Context context, Collector<Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>> collector) throws Exception {
        Tuple5<Boolean, Integer, Integer, Integer, Date> oldLeft = leftState.value();

        if ((oldLeft == null && left.f0) || (oldLeft != null && !left.f0)) {
            leftState.update(left);
            for (LineItem lineItem : lineItemListState.get()) {
                boolean update = OutputBoolean.isUpdate(left.f0, lineItem.update);
                Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer> tuple = Tuple8.of(update, left.f1, left.f2, left.f3, left.f4, lineItem.extendedPrice, lineItem.discount, lineItem.suppKey);
                if (OutputBoolean.canCollect(left.f0, lineItem.update)) {
                    collector.collect(tuple);
                }
            }
        }
    }

    @Override
    public void processElement2(LineItem lineItem, CoProcessFunction<Tuple5<Boolean, Integer, Integer, Integer, Date>, LineItem, Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>>.Context context, Collector<Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>> collector) throws Exception {
        boolean canAddToList = lineItem.update;
        if (!canAddToList) {
            int insertCount = 0;
            int deleteCount = 0;
            for (LineItem li : lineItemListState.get()) {
                if (li.update) {
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
        lineItemListState.add(lineItem);
        Tuple5<Boolean, Integer, Integer, Integer, Date> left = leftState.value();
        if (left != null) {
            boolean update = OutputBoolean.isUpdate(left.f0, lineItem.update);
            Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer> tuple = Tuple8.of(update, left.f1, left.f2, left.f3, left.f4, lineItem.extendedPrice, lineItem.discount, lineItem.suppKey);
            if (OutputBoolean.canCollect(left.f0, lineItem.update)) {
                collector.collect(tuple);
            }
        }
    }
}
