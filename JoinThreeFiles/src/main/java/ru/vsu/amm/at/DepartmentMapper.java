package ru.vsu.amm.at;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DepartmentMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private Map<Integer, String> names = new HashMap<>();
    private IntWritable KEY = new IntWritable();
    private Text VALUE = new Text();
    static String TAG = "DEPT";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(";");
        int id = Integer.parseInt(values[0]);
        String name = names.get(id);
        if (name != null) {
            KEY.set(id);
            VALUE.set(TAG + ";" + name + "\t" + values[1]);
            context.write(KEY, VALUE);
        }
    }

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        try {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(files != null && files.length > 0) {
                for (Path filePath : files) {
                    readFile(filePath);
                }
            }
        } catch (IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    private void readFile(Path filePath) {
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
            String line;
            String[] tokens;
            while((line = bufferedReader.readLine()) != null) {
                tokens = line.split(";", 2);
                names.put(Integer.parseInt(tokens[0]), tokens[1].replace(";", "\t"));
            }
        } catch(IOException ex) {
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
        }
    }
}
