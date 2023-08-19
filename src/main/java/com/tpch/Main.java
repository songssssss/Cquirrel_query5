package com.tpch;
import com.tpch.POJO.Customer;
import com.tpch.POJO.LineItem;
import com.tpch.POJO.Orders;
import com.tpch.POJO.Region;
import com.tpch.POJO.Nation;
import com.tpch.POJO.Supplier;
import com.tpch.operators.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.sql.Date;

public class Main {
    private static final OutputTag<Customer> customerOutputTag = new OutputTag<Customer>("customer"){};
    private static final OutputTag<Orders> ordersOutputTag = new OutputTag<Orders>("orders"){};
    private static final OutputTag<LineItem> lineItemOutputTag = new OutputTag<LineItem>("lineitem"){};
    private static final OutputTag<Supplier> supplierOutputTag = new OutputTag<Supplier>("supplier"){};
    private static final OutputTag<Nation> nationOutputTag = new OutputTag<Nation>("nation"){};
    private static final OutputTag<Region> regionOutputTag = new OutputTag<Region>("region"){};

    public static String desiredregion = "AMERICA";
    public static Date startdate = Date.valueOf("1995-01-01");
    public static Date enddate = Date.valueOf(Date.valueOf("1995-01-01").toLocalDate().plusYears(1));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.addSource(new DataSource()).setParallelism(1);
        SingleOutputStreamOperator<String> splitStream = stream.process(new SpiltStreamFunc());

        DataStream<Customer> customerDataStream = splitStream.getSideOutput(customerOutputTag);
        DataStream<Orders> ordersDataStream = splitStream.getSideOutput(ordersOutputTag);
        DataStream<LineItem> lineItemDataStream = splitStream.getSideOutput(lineItemOutputTag);
        DataStream<Supplier> supplierDataStream = splitStream.getSideOutput(supplierOutputTag);
        DataStream<Nation> nationDataStream = splitStream.getSideOutput(nationOutputTag);
        DataStream<Region> regionDataStream = splitStream.getSideOutput(regionOutputTag);

        env.setParallelism(12);

        SingleOutputStreamOperator<Tuple8<Boolean, Integer, Integer, Integer, Date, Double, Double, Integer>> COLStream = customerDataStream
                .connect(ordersDataStream)
                .keyBy(c -> c.custKey, o -> o.custKey)
                .process(new CustomerOrderJoin())
                .filter(t -> t.f4.compareTo(startdate) >= 0
                        && t.f4.compareTo(enddate) < 0)
                .connect(lineItemDataStream)
                .keyBy(t1 -> t1.f3, l -> l.orderKey)
                .process(new COLineItemJoin());
                //.print();

            ///*
        regionDataStream
                .filter(r -> r.name.equals(desiredregion))
                .connect(nationDataStream)
                .keyBy(r -> r.regionKey, n -> n.regionKey)
                .process(new RegionNationJoin())
                .connect(supplierDataStream)
                .keyBy(t1 -> t1.f4, s -> s.nationKey)
                .process(new RNSupplierJoin())
                .connect(COLStream)
                .keyBy(t1 -> t1.f3, t2 -> t2.f2) // nationkey
                //.keyBy(t1 -> t1.f5, t2 -> t2.f7)
                .process(new RNSCOLJoin())
                //.filter(t->t.f3.equals(t.f5))
                .filter(t->t.f5.equals(t.f6)) //supplierkey
                .keyBy(t -> t.f2)
                //.timeWindow( Time.seconds(15), Time.seconds(5) )
                //.process(new CalculateRevenuePerNation())
                .process(new CalculateNationRevenue())
                .print();
            //*/

        env.execute();
    }

    public static class SpiltStreamFunc extends ProcessFunction<String, String> {
        @Override
        public void processElement(String line, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
            String[] fields = line.split("\t");
            String tag = fields[fields.length-1];

            switch (tag) {
                case "customer":
                    context.output(customerOutputTag, new Customer(fields));
                    break;
                case "orders":
                    context.output(ordersOutputTag, new Orders(fields));
                    break;
                case "lineitem":
                    context.output(lineItemOutputTag, new LineItem(fields));
                    break;
                case "supplier":
                    context.output(supplierOutputTag, new Supplier(fields));
                    break;
                case "nation":
                    context.output(nationOutputTag, new Nation(fields));
                    break;
                case "region":
                    context.output(regionOutputTag, new Region(fields));
                    break;
                default:
                    break;
            }
        }
    }
}
