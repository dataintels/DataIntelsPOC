package sparkPOC;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.json.JSONObject;
import org.json.XML;

import scala.Tuple2;

import com.mongodb.hadoop.MongoOutputFormat;



public class XmlReader {
	
	private static final Pattern TAG_REGEX = Pattern.compile("<catalog>(.+?)</catalog>");

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "F:/StudyMaterials/winutil/");//Path needs to be modified according to the file location in ur system
		String logMessages=null;
		Path filePath = Paths.get("F:/WorkSpace/sparkPOC/src/main/resources/","test.txt");//Path needs to be modified according to the file location in ur system
		try {
		      byte[] textArray = Files.readAllBytes(filePath);

		      logMessages = new String(textArray);
		    } catch (IOException e) {
		      System.out.println(e);
		    }
	    String formattedLogMessages = logMessages.replaceAll("(\r\n|\n)", "");
	    List<String> atomMessageList=new ArrayList<String>();
	    atomMessageList=getTagValues(formattedLogMessages);
	    SparkConf conf = new SparkConf().setAppName("Spark POC Application");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> distAtomData=sc.parallelize(atomMessageList);
	    JavaPairRDD<Object, BSONObject> distAtomDataPairs = distAtomData.mapToPair(new PairFunction<String,Object,BSONObject>()
	    		{
					private static final long serialVersionUID = 1L;

						public Tuple2<Object,BSONObject> call(String xmlContent) 
						{ 							
							BSONObject bson = new BasicBSONObject();
							String startTag="<?xml version=\"1.0\" encoding=\"utf-8\"?><catalog>";
							String endTag="</catalog>";
							JSONObject xmlJSONObj = XML.toJSONObject(startTag+xmlContent+endTag);
							BSONObject bsonObject = (BSONObject)com.mongodb.util.JSON.parse(xmlJSONObj.toString());
							bson.putAll(bsonObject);
			                return new Tuple2<Object,BSONObject>(null, bson);
						}
	    		}	    		
	    		);
	    Configuration mongodbConfig = new Configuration();
	    mongodbConfig.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
	    mongodbConfig.set("mongo.output.uri","mongodb://localhost:27017/catalog.books");
	    distAtomDataPairs.saveAsNewAPIHadoopFile(
	    	    "",
	    	    Object.class,
	    	    Object.class,
	    	    MongoOutputFormat.class,
	    	    mongodbConfig
	    	);
	}

	

	private static List<String> getTagValues(String str) {
	    final List<String> tagValues = new ArrayList<String>();
	    final Matcher matcher = TAG_REGEX.matcher(str);
	    while (matcher.find()) {
	        tagValues.add(matcher.group(1));
	    }
	    return tagValues;
	}
}
