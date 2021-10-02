import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class FourthTask {

	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		 if (args.length < 1) {
		System.err.println("Usage: JavaWordCount <file>");
		System.exit(1);
		}

	 	SparkSession spark = SparkSession
	 		.builder()
	 		.appName("Test")
	 		.getOrCreate();
		 	
	 	SparkContext sc = spark.sparkContext(); 
	 	
	 	Dataset<Row> csvFileDF = spark.read().format("csv")
	 		    .option("header", "true")
	 		    .option("delimiter", ",")
	 		    .load(args[0]);
	 	
	 	// sourceLogPaths is an array of different file names
	 	JavaRDD<String> textFile = spark.read().textFile(args[0]).javaRDD();
	 	JavaRDD<Row> rowRDD = textFile.map(RowFactory::create);
	 	// How to add a field that shows the associated filename for each row?
	 	List<StructField> fields = Arrays.asList(DataTypes.createStructField("line", DataTypes.StringType, true)); 
	 	StructType schema = DataTypes.createStructType(fields);
	 	SQLContext sqlContext = spark.sqlContext();

	 	// Below line has the additional column added
	 	Dataset<Row> df = sqlContext.createDataFrame(rowRDD, schema).withColumn("file_name", functions.input_file_name());
	 	df.show();
	 	
	 	JavaRDD<Row> lines = df.javaRDD();
	 	
	 	JavaPairRDD<String, String> pairedLines =lines.mapToPair(new PairFunction<Row, String, String>() {

			@Override
			public Tuple2<String, String> call(Row row) throws Exception {
				// TODO Auto-generated method stub
				
				
				String[] pathParts = row.getAs("file_name").toString().split("/");
				String[] nameParts = pathParts[pathParts.length-1].split("\\.");
				String line = row.getAs("line");
				String[] tokens = line.split("\\,");
				
				try {
					String type, h, u, res;
					if(nameParts[0].equals("stationlog")) {
						type = "sta";
						h = nameParts[1];
						
						int minute = (((int)Double.parseDouble(tokens[1])-60*60*24*6)/60/5)*5;
						int day = (((int)Double.parseDouble(tokens[1]))/60/60/24)+1;
						if(day == 7) {
							Tuple2<String, String> result = new Tuple2<String, String>("sta|"+h+"|"+minute+"|", tokens[2]+"|"+"1");
							return result;
						}else {
							Tuple2<String, String> result = new Tuple2<String, String>("error|sta", "not a seven day");
							return result;
						}
					}
					else if(nameParts[0].equals("userlog")) {
						type = "usr";
						h = nameParts[1];
						u = nameParts[2];
						int minute = (((int)Double.parseDouble(tokens[1]))/60/5)*5;
						if(tokens[0].equals("7")) {
							Tuple2<String, String> result = new Tuple2<String, String>("usr|"+u+"|"+h+"|"+minute+"|", tokens[2]+"|"+"1");
							return result;
						}else {
							Tuple2<String, String> result = new Tuple2<String, String>("error|usr", "not a seven day");
							return result;
						}
					}else {
						Tuple2<String, String> result = new Tuple2<String, String>("error", "not a user and not a station");
						return result;
					}
				}catch(Exception e){
					Tuple2<String, String> result = new Tuple2<String, String>("error", e.getMessage());
					return result;
				}					
			}
		});
	 	
	 	JavaPairRDD<String, String> pairedLines2 = pairedLines.filter(new Function<Tuple2<String,String>, Boolean>() {
			
			@Override
			public Boolean call(Tuple2<String, String> pair) throws Exception {
				return !pair._1().contains("error");
			}
		});
	 	
	 	JavaPairRDD<String, String> pairedLines3 = pairedLines2.reduceByKey(new Function2<String, String, String>() {
			
			@Override
			public String call(String accum, String newVal) throws Exception {
				// TODO Auto-generated method stub
				
				String[] accumArr = accum.split("\\|");
				String[] valArr = newVal.split("\\|");
				
				Double curval = Double.parseDouble(valArr[0]);
				int cur_amount = Integer.parseInt(valArr[1]);
				
				Double sum = Double.parseDouble(accumArr[0]);
				int amount = Integer.parseInt(accumArr[1]);
				
				sum += curval;
				amount += cur_amount;
				
				return sum+"|"+amount;
			}
		});
	 	
	 	
	 	JavaPairRDD<String, String> pairedLines4 = pairedLines3.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				String[] tokens = (t._1()+t._2()).split("\\|");
				try{
					String type = tokens[0];
					if(type.equals("usr")) {
						String h = tokens[2];
						String minute = tokens[3];
						Double avg = Double.parseDouble(tokens[4])/Integer.parseInt(tokens[5]);
						Tuple2<String, String> result = new Tuple2<String, String>(h+"|"+minute+"|", avg+"|0.0");
						return result;
					}else if(type.equals("sta")) {
						String h = tokens[1];
						String minute = tokens[2];
						Double avg = Double.parseDouble(tokens[3])/Integer.parseInt(tokens[4]);
						Tuple2<String, String> result = new Tuple2<String, String>(h+"|"+minute+"|", "0.0|"+avg);
						return result;
					}else {
						Tuple2<String, String> result = new Tuple2<String, String>("error", type+" is wrong type");
						return result;
					}
				}catch(Exception e){
					Tuple2<String, String> result = new Tuple2<String, String>("error", e.getMessage());
					return result;
				}
			}
	 		
		});
		
	 	JavaPairRDD<String, String> pairedLines5 = pairedLines4.reduceByKey(new Function2<String, String, String>() {
			
			@Override
			public String call(String accum, String newVal) throws Exception {
				// TODO Auto-generated method stub
				
				String[] accumArr = accum.split("\\|");
				String[] valArr = newVal.split("\\|");
				
				Double sumSpeed = Double.parseDouble(accumArr[0]);
				Double sumErr = Double.parseDouble(accumArr[1]);
				
				Double Speed = Double.parseDouble(valArr[0]);
				Double err = Double.parseDouble(valArr[1]);
				
				sumSpeed += Speed;
				sumErr += err;
				
				return sumSpeed+"|"+sumErr;
			}
		});
	 	
	 	JavaRDD<String> resultLines = pairedLines5.map(new Function<Tuple2<String,String>, String>() {

			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				// TODO Auto-generated method stub
				return v1._1()+v1._2();
			}
		});
	 	
	 	resultLines.saveAsTextFile(args[1]);
		
		
		spark.stop();
		
	}

}
