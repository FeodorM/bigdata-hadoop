package ru.vsu.amm.at;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MapperReducerTest {

    private MapDriver<LongWritable, Text, Text, Text> mapDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp() throws Exception {
        MyMapper mapper = new MyMapper();
        MyReducer reducer = new MyReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new LongWritable(), new Text(
                "79036541234\t79013454321\t15"));
        mapDriver.withOutput(new Text("79036541234"), new Text("79013454321\t15"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("79013454321\t15"));
        values.add(new Text("79013454321\t15"));
        values.add(new Text("79013454322\t16"));
        reduceDriver.withInput(new Text("79036541234"), values);
        reduceDriver.withOutput(new Text("79036541234"), new Text("79013454321"));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "79036541234\t79013454321\t15"));
//        List<> values = new ArrayList<>();
//        values.add(new LongWritable(1));
//        values.add(new LongWritable(1));
        mapReduceDriver.withOutput(new Text("79036541234"), new Text("79013454321"));
        mapReduceDriver.runTest();
    }

}