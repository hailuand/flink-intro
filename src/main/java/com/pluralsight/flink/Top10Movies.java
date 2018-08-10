package com.pluralsight.flink;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;


public class Top10Movies {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Long, Double>> sorted = env.readCsvFile("ml-latest-small/ratings.csv")
                .ignoreFirstLine()
                .includeFields(false, true, true, false)// movie id, rating
                .types(Long.class, Double.class)
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Long, Double>> ratings,
                                       Collector<Tuple2<Long, Double>> collector) throws Exception {
                        Long movieId = null;
                        double total = 0;
                        int count = 0;
                        for (Tuple2<Long, Double> value : ratings) {
                            movieId = value.f0;
                            total += value.f1;
                            count++;
                        }

                        if (count > 50) {
                            collector.collect(new Tuple2<>(movieId, total / count));
                        }
                    }
                })
                .partitionCustom((Partitioner<Double>) (key, numPartitions) -> key.intValue() - 1, 1)
                .setParallelism(5)
                .sortPartition(1, Order.DESCENDING)
                .mapPartition(new MapPartitionFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Long, Double>> values,
                                             Collector<Tuple2<Long, Double>> collector) throws Exception {
                        Iterator<Tuple2<Long, Double>> iter = values.iterator();
                        for (int i = 0; i < 10 && iter.hasNext(); i++) {
                            collector.collect(iter.next());
                        }
                    }
                })
                .sortPartition(1, Order.DESCENDING)
                .setParallelism(1)
                .mapPartition(new MapPartitionFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Long, Double>> values,
                                             Collector<Tuple2<Long, Double>> collector) throws Exception {
                        Iterator<Tuple2<Long, Double>> iter = values.iterator();
                        for (int i = 0; i < 10 && iter.hasNext(); i++) {
                            collector.collect(iter.next());
                        }
                    }
                });


        DataSet<Tuple2<Long, String>> movies = env.readCsvFile("ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .includeFields(true, true, false)
                .types(Long.class, String.class);

        sorted.join(movies)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Double>, Tuple2<Long, String>, Tuple3<Long, String, Double>>() {
                    @Override
                    public Tuple3<Long, String, Double> join(Tuple2<Long, Double> first,
                                                             Tuple2<Long, String> second) throws Exception {
                        return(new Tuple3<>(first.f0, second.f1, first.f1));
                    }
                })
                .print();
    }
}
