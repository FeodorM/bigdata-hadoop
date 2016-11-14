package ru.vsu.amm.at;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapperReducerTest {

    private SalaryMapper salaryMapper = new SalaryMapper();
    private DepartmentMapper deptMapper = new DepartmentMapper();
    private MapDriver<LongWritable, Text, IntWritable, Text> salaryMapDriver;
    private MapDriver<LongWritable, Text, IntWritable, Text> deptMapDriver;
    private ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;
    private MultipleInputsMapReduceDriver<IntWritable, Text, IntWritable, Text> mapReduceDriver;

    @Before
    public void setUp() throws Exception {
        MyReducer reducer = new MyReducer();
        salaryMapDriver = MapDriver.newMapDriver(salaryMapper);
        deptMapDriver = MapDriver.newMapDriver(deptMapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer);
        mapReduceDriver.addMapper(salaryMapper);
        mapReduceDriver.addMapper(deptMapper);
    }

    @Test
    public void salaryMapper() throws IOException {
        salaryMapDriver.withInput(new LongWritable(), new Text("1;500"));
        salaryMapDriver.withOutput(new IntWritable(1), new Text("SAL;500"));
        salaryMapDriver.runTest();
    }

    @Test
    public void departmentMapper() {
        deptMapDriver.addCacheFile("names.txt");
        deptMapDriver.withInput(new LongWritable(), new Text("1;first"));
        deptMapDriver.withOutput(new IntWritable(1), new Text("DEPT;Ivan\tIvanov\tfirst"));
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("SAL;500"));
        values.add(new Text("DEPT;Ivan\tIvanov\tfirst"));
        reduceDriver.withInput(new IntWritable(1), values);
        reduceDriver.withOutput(new IntWritable(1), new Text("Ivan\tIvanov\tfirst\t500"));
        reduceDriver.runTest();
    }

    @Ignore
    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(salaryMapper, new LongWritable(), new Text("1;500"));
        mapReduceDriver.withInput(deptMapper, new LongWritable(), new Text("1;dept"));
        mapReduceDriver.addCacheFile("names.txt");
        mapReduceDriver.withOutput(new IntWritable(1), new Text("Ivan;Ivanov;dept;500"));
        mapReduceDriver.runTest();
    }

}