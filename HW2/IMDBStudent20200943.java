import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
public class IMDBStudent20200943{
	public static class Movie{
		String title;
		double rating;

		public Movie(String title, double rating){
			this.title = title;
			this.rating = rating;
		}

		public String getTitle(){return title;}
		public double getRating(){return rating;}

	}
	public static class MovieComparator implements Comparator<Movie> {
		public int compare(Movie x, Movie y) {
			if ( x.rating > y.rating ) return 1;
			if ( x.rating < y.rating ) return -1;
			return 0;
		}
	}

	//우선순위 큐에 넣는 작업
	public static void insertMovie(PriorityQueue q, String title, double rating, int topK) {
		Movie movie_head = (Movie) q.peek();

		if ( q.size() < topK || movie_head.rating < rating ){
			Movie movie = new Movie(title, rating);
			q.add( movie );
			if( q.size() > topK ) q.remove();
		}
	}

	public static class IMDBStudent20200943Mapper extends Mapper<Object, Text, Text, Text>{
		//파일 이름으로 분류
		boolean movieFile = true;
		protected void setup(Context context) throws IOException, InterruptedException
		{
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if ( filename.indexOf( "movies" ) != -1 ) movieFile = true;
			else movieFile = false;

			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
		}

		private Text output_key = new Text();
		private Text output_value = new Text();
		private int topK;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			
			String[] sp = value.toString().split("::");
			String tableName;
			String movieId; //joinKey

			if(movieFile){ //input: [movieid::title::genre]
				tableName = "m";
				movieId = sp[0];
				String title = sp[1];
				String genre = sp[2];
				output_value.set(String.format("%s::%s::%s", tableName, title, genre));

			}else{ //input: [userid::movieid::rating::xxx
				tableName = "d";
				movieId = sp[1];
				String rating = sp[2];
				output_value.set(String.format("%s::%s", tableName, rating));
			}

			output_key.set(movieId);
			context.write(output_key, output_value);
		}
	}

	public static class IMDBStudent20200943Reducer extends Reducer<Text,Text,Text,DoubleWritable> {
		private PriorityQueue<Movie> queue ;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Movie>( topK , comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			while( queue.size() != 0 ) {
				Movie movie = (Movie) queue.remove();
				context.write( new Text( movie.getTitle() ), new DoubleWritable(movie.getRating()) );
			}
		}

		private Text outputKey = new Text();
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Text reduce_key = new Text();
			DoubleWritable reduce_result = new DoubleWritable();
			String title = "";
			ArrayList<String> buffer = new ArrayList<String>();
			double avg = 0;
			int size =0;

			String genre ="";
			for (Text val : values) {
				String file_type;
				String[] str = val.toString().split("::");
				file_type = str[0];
				
				if( file_type.equals( "m" ) )  {
					title = str[1];
					genre = str[2];
				}else{
					if ( title.length() == 0 ) {
						buffer.add( val.toString() );
					}else {
						double d= Double.parseDouble(str[1]);
						avg+=d;
						size++;
					}
				}
			}

			for ( int i = 0 ; i < buffer.size(); i++ ){
				String [] strs = buffer.get(i).split("::");
				double d= Double.parseDouble(strs[1]);
				avg+=d;
				size++;
			}
			if(size !=0)
				avg = avg/size;
				avg = Math.round(avg*10)/10.0;
			
			reduce_key.set(title);
			reduce_result.set(avg);

<<<<<<< HEAD
			if(genre.contains("Fantasy"))
=======
			if(genre.equals("Fantasy"))
>>>>>>> 06c88a308ad6dbd98fbd554b4993c03180d8945b
					insertMovie(queue, title, avg, topK);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) 
		{
			System.err.println("Usage: IMDBStudent20200943 <in> <out> <k>");
			System.exit(2);
		}

		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		Job job = new Job(conf, "IMDBStudent20200943");
		job.setJarByClass(IMDBStudent20200943.class);
		job.setMapperClass(IMDBStudent20200943Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(IMDBStudent20200943Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
