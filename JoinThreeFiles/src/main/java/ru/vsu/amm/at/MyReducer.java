package ru.vsu.amm.at;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private Text VALUE = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String name = null;
        String salary = null;
        for (Text value : values) {
            String[] tokens = value.toString().split(";", 2);
            if (tokens[0].equals(SalaryMapper.TAG)) {
                salary = tokens[1];
            } else if (tokens[0].equals(DepartmentMapper.TAG)) {
                name = tokens[1];
            }
        }
        if (name != null && salary != null) {
            VALUE.set(name + "\t" + salary);
            context.write(key, VALUE);
        }
    }
}
