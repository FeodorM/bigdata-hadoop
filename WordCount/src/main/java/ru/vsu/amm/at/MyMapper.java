package ru.vsu.amm.at;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Test mapper for hadoop
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text wordToWrite = new Text();
    private Set<String> stopWords = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(stopWordsFiles != null && stopWordsFiles.length > 0) {
                for (Path stopWordFile : stopWordsFiles) {
                    readFile(stopWordFile);
                }
            }
        } catch (IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t", -1);
        for (String word : words) {
            if (!stopWords.contains(word)) {
                wordToWrite.set(word);
                context.write(wordToWrite, one);
            }
        }
    }

    private void readFile(Path filePath) {
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
            String stopWord = null;
            while((stopWord = bufferedReader.readLine()) != null) {
                stopWords.add(stopWord.toLowerCase());
            }
        } catch(IOException ex) {
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
        }
    }
}
