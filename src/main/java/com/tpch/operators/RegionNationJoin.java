package com.tpch.operators;
import com.tpch.POJO.Nation;
import com.tpch.POJO.Region;
import com.tpch.utils.OutputBoolean;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;


public class RegionNationJoin extends CoProcessFunction<Region, Nation, Tuple5<Boolean, String, Integer, String, Integer>> {
    private ValueState<Region> regionState;
    private ListState<Nation> nationListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        regionState = getRuntimeContext().getState(
                new ValueStateDescriptor<Region>("regionState", Region.class)
        );
        nationListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Nation>("nationState", Nation.class)
        );
    }

    @Override
    public void processElement1(Region region, CoProcessFunction<Region, Nation, Tuple5<Boolean, String, Integer, String, Integer>>.Context context, Collector<Tuple5<Boolean, String, Integer, String, Integer>> collector) throws Exception {
        Region oldRegion = regionState.value();

        if ((oldRegion == null && region.update) || (oldRegion != null && !region.update)) {
            regionState.update(region);
            for (Nation nation : nationListState.get()) {
                boolean update = OutputBoolean.isUpdate(region.update, nation.update);
                Tuple5<Boolean, String, Integer, String, Integer> tuple = Tuple5.of(update, region.name, region.regionKey, nation.name, nation.nationKey);
                if (OutputBoolean.canCollect(region.update, nation.update)) {
                    collector.collect(tuple);
                }
            }
        }
    }

    @Override
    public void processElement2(Nation nation, CoProcessFunction<Region, Nation, Tuple5<Boolean, String, Integer, String, Integer>>.Context context, Collector<Tuple5<Boolean, String, Integer, String, Integer>> collector) throws Exception {
        boolean canAddToList = nation.update;
        if (!canAddToList) {
            int insertCount = 0;
            int deleteCount = 0;
            for (Nation n : nationListState.get()) {
                if (n.update) {
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
        nationListState.add(nation);
        Region region = regionState.value();
        if (region != null) {
            boolean update = OutputBoolean.isUpdate(region.update, nation.update);
            Tuple5<Boolean, String, Integer, String, Integer> tuple = Tuple5.of(update, region.name, region.regionKey, nation.name, nation.nationKey);
            if (OutputBoolean.canCollect(region.update, nation.update)) {
                collector.collect(tuple);
            }
        }
    }
}

