package ru.vsu.amm.at;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalaryMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private IntWritable KEY = new IntWritable();
    private Text VALUE = new Text();
    static final String TAG = "SAL";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(";", -1);
        KEY.set(Integer.parseInt(values[0]));
        VALUE.set(TAG + ";" + values[1]);
        context.write(KEY, VALUE);
    }
}
