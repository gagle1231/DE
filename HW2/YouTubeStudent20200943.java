import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20200943{

	public static class Category {
		public String name;
		public double rating;
		
		public Category(String name, double rating){
			this.name = name;
			this.rating = rating;
		}
		
		public String getName(){return name;}
		public double getRating(){return rating;}
		public String getString(){
			return name+" "+rating;
		}
		
	}
	
	public static class CategoryComparator implements Comparator<Category> {
		public int compare(Category x, Category y) {
			if ( x.rating > y.rating ) return 1;
			if ( x.rating < y.rating ) return -1;
			return 0;
		}
	}
	public static void insertCategory(PriorityQueue q, String name, double rating, int topK) {
		Category cat_head = (Category) q.peek();
		if ( q.size() < topK || cat_head.rating < rating )
		{
			Category cat = new Category(name, rating);
			q.add( cat );
			if( q.size() > topK ) q.remove();
		}
	}

	public static class YouTubeStudent20200943Mapper extends Mapper<Object, Text, Text, DoubleWritable> {
		public void map(Object key, Text value, Context context) throws IOException, 
		       InterruptedException {
		       //47EWHY3E5AM|MgsTheFury404|1024|Entertainment|123|111371|4.77
			       String[] str = value.toString().split("\\|");
			       String cat_list = str[3];
			       String rating = str[6];
			       
			       StringTokenizer st = new StringTokenizer(cat_list, " & ");
			       while(st.hasMoreTokens()){
			       	String cat_name = st.nextToken();
			       	context.write( new Text( cat_name ), new DoubleWritable(Double.parseDouble(rating)) ); 
			       }
		}


	}

	public static class YouTubeStudent20200943Reducer extends Reducer<Text,DoubleWritable,Text,NullWritable> {
			private PriorityQueue<Category> queue ;
			private Comparator<Category> comp = new CategoryComparator();
			private int topK;
			public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws 
				IOException, InterruptedException  {  
					String cat_name = key.toString();
					double avg = 0;
					int cnt = 0;
					
					for(DoubleWritable d: values){
						avg += d.get(); cnt++;	
					}
					if(cnt!=0)
						avg = avg/cnt;
					insertCategory(queue, cat_name, avg, topK);
				}

			protected void setup(Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				topK = conf.getInt("topK", -1);
				queue = new PriorityQueue<Category>( topK , comp);
			}
			protected void cleanup(Context context) throws IOException, InterruptedException {
				while( queue.size() != 0 ) {
					Category cat = (Category) queue.remove();
					context.write( new Text( cat.getString() ), new NullWritable.get() );
				}
			}

		}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("Usage: YouTubeStudent20200943 <in> <out> <topK>");   System.exit(2);
		}
		int topK = Integer.parseInt(otherArgs[2]);
		conf.setInt("topK", topK);
		Job job = new Job(conf, "YouTubeStudent20200943");
		job.setJarByClass(YouTubeStudent20200943.class);
		job.setMapperClass(YouTubeStudent20200943Mapper.class);
		job.setReducerClass(YouTubeStudent20200943Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
