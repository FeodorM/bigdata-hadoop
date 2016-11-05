package ru.vsu.amm.at;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Long> map = new HashMap<String, Long>();
        for (Text value : values) {
            String[] keyVal = value.toString().split("\t");
            if (!map.containsKey(keyVal[0])) {
                map.put(keyVal[0], Long.parseLong(keyVal[1]));
            } else {
                map.put(keyVal[0], map.get(keyVal[0]) + Long.parseLong(keyVal[1]));
            }
        }
        long max = -1;
        String number = "";
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            if (entry.getValue() > max) {
                max = entry.getValue();
                number = entry.getKey();
            }
        }
        context.write(key, new Text(number));
    }
}
