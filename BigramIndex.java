import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigramIndex {
   public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>
   {
      //private final static IntWritable one = new IntWritable(1);
      private Text phrase = new Text();
      private Text document_ID = new Text();

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException
      {
         HashSet<String> given_bigrams = new HashSet<>();
         given_bigrams.add("computer science");
         given_bigrams.add("information retrieval");
         given_bigrams.add("power politics");
         given_bigrams.add("los angeles");
         given_bigrams.add("bruce willis");

         // Split into array of size 2; first element is document ID, second element is the rest of the text
         String[] doc_text_arr = value.toString().split("\t",2);
         document_ID.set(doc_text_arr[0]);

         // convert text to lower case
         String word_text = doc_text_arr[1].toLowerCase();
         // replace non-alphabet symbols with spaces
         word_text = word_text.replaceAll("[^a-zA-Z]+", " ");

         StringTokenizer itr = new StringTokenizer(word_text);
         String first_word = itr.nextToken();
         while (itr.hasMoreTokens())
         {
            String second_word = itr.nextToken();
            String bigram = first_word + " " + second_word;
            if(given_bigrams.contains(bigram)){
               phrase.set(bigram);
               context.write(phrase, document_ID);
            }
            first_word = second_word;
         }
      }
   }

   public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>
   {
      //private IntWritable result = new IntWritable();
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
      {
         // count how many times a document shows up for the word
         HashMap<String, Integer> docID_count = new HashMap<>();
         for(Text doc_ID: values){
            String id = doc_ID.toString();
            docID_count.put(id, docID_count.getOrDefault(id, 0) + 1);
         }

         // create the final output - word == string of documents + # of occurrences
         StringBuilder result = new StringBuilder();
         for(Map.Entry<String, Integer> entry : docID_count.entrySet()){
            if(result.length() > 0){
               result.append(" ");
            }
            String add_result = entry.getKey() + ":" + entry.getValue();
            result.append(add_result);
         }

         context.write(key, new Text(String.valueOf(result)));
      }
   }

   public static void main(String[] args) throws Exception
   {
      Configuration conf = new Configuration();
      conf.set("mapred.min.split.size", "134217728");
      Job job = Job.getInstance(conf, "word count");

      job.setJarByClass(BigramIndex.class);
      job.setMapperClass(InvertedIndexMapper.class);
      //job.setCombinerClass(IntSumReducer.class);
      job.setReducerClass(InvertedIndexReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}// WordCount

