package edu.stanford.cs246.wordcount;


import java.util.*;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Ex3 extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Ex3(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
	  //Create a intermediate path for the output of mapreduce 1
      String intermediate_file_path = "tmp/part-r-00000";
      
      Configuration conf_1 = this.getConf();
      Job job1 = Job.getInstance(conf_1, "map_reduce_1");
      
      job1.setJarByClass(Ex3.class);
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(IntWritable.class);

      job1.setMapperClass(Map_1.class);
      job1.setReducerClass(Reduce_1.class);

      job1.setInputFormatClass(TextInputFormat.class);
      job1.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job1, new Path(args[0]));
      FileOutputFormat.setOutputPath(job1, new Path(intermediate_file_path));

      job1.waitForCompletion(true);
      
      Configuration conf_2 = this.getConf();
      Job job2 = Job.getInstance(conf_2, "map_reduce_2");
      
      job2.setJarByClass(Ex3.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);

      job2.setMapperClass(Map_2.class);
      job2.setReducerClass(Reduce_2.class);

      job2.setInputFormatClass(TextInputFormat.class);
      job2.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job2, new Path(intermediate_file_path));
      FileOutputFormat.setOutputPath(job2, new Path(args[1]));

      job2.waitForCompletion(true);
      
      
      return 0;
   }
  
   
   public static class Map_1 extends Mapper<LongWritable, Text, Text, IntWritable> {
      
	   
	  public final static IntWritable Is_Friend = new IntWritable(-1);
	  public final static IntWritable ONE = new IntWritable(1);
	  
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	  String line[] = value.toString().split("\t"); //split the user_id and fd list
    	  String user_id = line[0];
    	  
    	  if (line.length == 2) { //if the user has any friend on the list
    		  String[] friend_list = line[1].split(","); //split the fd list
    		  
    		  //write down -1 if two user ids are friend or 1 if they have mutual friend
    		  //in form of key: {user_id_1,userid_2}, value{-1 or 1} 
    		  
    		  for(int i=0; i<friend_list.length;i++) {  
    			  //If they are friend, mark as -1
    			  context.write(new Text(user_id + "," + friend_list[i] ), Is_Friend);

    			  for (int j = i + 1; j < friend_list.length; j++) {
    				  //For every friend A and B in the list have one mutual fd
						context.write(new Text(friend_list[i] + "," + friend_list[j]) , ONE);
						context.write(new Text(friend_list[j] + "," + friend_list[i]) , ONE);
					}
    		  	}
    	  	}
      	}
      
   }

   public static class Reduce_1 extends Reducer<Text, IntWritable, Text, Text> {
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
      
    	 for (IntWritable val : values){
    		 //count the number of mutual fd of each id pair
    		 if (val.get() == 1)
    			 sum += val.get();
    		 //remove the id pair if they are fd
    		 else if (val.get() == -1){
    			 return;
    		 }
    	 }
    	 String user_id_1 = key.toString().split(",")[0];
    	 String user_id_2 = key.toString().split(",")[1];
    	 //rewirte the key and value pair in form of key:{user_id_1}, value:{user_id_2,number_of_mutual_fd}
    	 context.write(new Text(user_id_1), new Text(user_id_2 + "," + Integer.toString(sum)));

      }
   }
   
   
   public static class Map_2 extends Mapper<LongWritable, Text, Text, Text> {
	      
	      public void map(LongWritable key, Text values, Context context)
	              throws IOException, InterruptedException {

	    	  String key_ID_value[] = values.toString().split("\t");
	    	  //rewirte back the key and value pair in form of key:{user_id_1}, value:{user_id_2,number_of_mutual_fd}
	    	  context.write(new Text(key_ID_value[0]), new Text(key_ID_value[1]));

	      }
	   }

	   public static class Reduce_2 extends Reducer<Text, Text, Text, Text> {
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	         
	    	  List<ArrayList<String>> mutual_fd_list = new ArrayList<ArrayList<String>>();
	    	  for(Text val:values ){
	    		  //split the user id and the number of mutual fd
	        	  String mutual_fd_information[] = val.toString().split(",");
	    		  //add them into mutual fd list
	    		  mutual_fd_list.add(new ArrayList<String>(Arrays.asList(mutual_fd_information)));
	    	  }
	    	  //sort the list 
	    	  Collections.sort(mutual_fd_list, new Comparator<ArrayList<String>>(){
	    		  @Override
	    		  public int compare(ArrayList<String> a, ArrayList<String> b){
	    			  
	    			  int mutual_fd_num_1 = Integer.parseInt(a.get(1));
	    			  int mutual_fd_num_2 = Integer.parseInt(b.get(1));
	    			  int user_id_1 = Integer.parseInt(a.get(0));
	    			  int user_id_2 = Integer.parseInt(b.get(0));
	    			  
	    			  if (mutual_fd_num_2 - mutual_fd_num_1 == 0)
	    				  return user_id_1 - user_id_2;
	    			  else 
	    				  return mutual_fd_num_2 - mutual_fd_num_1;
	    		  }
	    	  });
	    	  

	    	  /*
	    	  List<String> user_list = new ArrayList<String>()  {{
		    		 add("924");
		    		 add("8941");
		    		 add("8942");
		    		 add("9019");
		    		 add("9020");
		    		 add("9021");
		    		 add("9022");
		    		 add("9990");
		    		 add("9992");
		    		 add("9993");
		    	 }};
		    	 */
	    	  
	    	  //output the at max 10 recommend fd
	    	  String output_id ="";
	    	  for (int i =0 ; i  < mutual_fd_list.size();i++){
	    		  if (i == 10)
	    			  break;
	    		  output_id += mutual_fd_list.get(i).get(0);
	    				  
	    		  if (i != 9 & i!= mutual_fd_list.size() - 1){
	    			  output_id += ",";
	    		  }
	    	  }
	    	  
	    	  context.write(new Text(key.toString()), new Text(output_id));

	      }
	   }
	   
}
