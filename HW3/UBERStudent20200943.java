import scala.Tuple2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
public class UBERStudent20200943 implements Serializable {
	public static String[] dayStr = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: UBERStudent20200943 <in-file> <out-file>");
			System.exit(1);
			}
			SparkSession spark = SparkSession
			.builder()
			.appName("JavaWordCount")
			.getOrCreate();
			JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
			
		
			PairFunction<String, String, String> pf = new PairFunction<String, String, String>() {
				public Tuple2<String, String> call(String s) {
					String[] strs = s.split(",");
					Date date = new Date(strs[1]);
					String day = dayStr[date.getDay()];
					String key = String.format("%s,%s", strs[0], day);
					String value = String.format("%s,%s", strs[3], strs[2]);
					return new Tuple2(key, value);
				}
				};
			JavaPairRDD<String, String> maps = lines.mapToPair(pf);
			
			Function2<String, String, String> f2 = new Function2<String, String, String>() {
				public String call(String x, String y) {
					String[] xData = x.split(",");
					String[] yData = y.split(",");
					int totalTrips = Integer.parseInt(xData[0]) + Integer.parseInt(yData[0]);
					int totalVehicles = Integer.parseInt(xData[1]) + Integer.parseInt(yData[1]);
					
					return String.format("%s,%s", totalTrips, totalVehicles);
				}
				};
				JavaPairRDD<String, String> rslt = maps.reduceByKey(f2);
				rslt.saveAsTextFile(args[1]);
				spark.stop();
			
	}
	
	
}
