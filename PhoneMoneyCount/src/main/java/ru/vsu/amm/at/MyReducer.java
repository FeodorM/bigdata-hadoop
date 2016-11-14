package ru.vsu.amm.at;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, String[]> persons = new HashMap<>();
    private Map<String, String> cities = new HashMap<>();
    private Text VALUE = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Long> map = new HashMap<>();
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
        VALUE.set(number);
        enrichData(key);
        enrichData(VALUE);
        context.write(key, VALUE);
    }

    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        try {
            Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(stopWordsFiles != null && stopWordsFiles.length > 0) {
                for (Path stopWordFile : stopWordsFiles) {
                    if (stopWordFile.getName().equals("city.txt")) {
                        readCities(stopWordFile);
                    } else if (stopWordFile.getName().equals("person.txt")) {
                        readPersons(stopWordFile);
                    }
                }
            }
        } catch (IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    private void readCities(Path filePath) {
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
            String line;
            String[] tokens;
            while((line = bufferedReader.readLine()) != null) {
                tokens = line.split(" ");
                cities.put(tokens[0], tokens[1]);
            }
        } catch(IOException ex) {
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
        }
    }

    private void readPersons(Path filePath) {
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
            String line;
            String[] tokens;
            while((line = bufferedReader.readLine()) != null) {
                tokens = line.split(" ");
                persons.put(tokens[0], Arrays.copyOfRange(tokens, 1, tokens.length));
            }
        } catch(IOException ex) {
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
        }
    }

    private void enrichData(Text number) {
        String num = number.toString();
        String[] data = persons.get(num);
        number.set(data[0] + " " + data[1] + " " + cities.get(data[2]));
    }
}
