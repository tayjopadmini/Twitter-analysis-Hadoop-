import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Date;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TimeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private IntWritable data = new IntWritable();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String tweet[]=value.toString().split(";");                                                   //converts the input line into a String and splits it on ;. These values are stored in tweet
    	if (tweet.length==4)                                                                          //Data cleaning. checks if the srtring has 4 parts after splitting on a ;
    	{
            try {
            	long EpochTime=Long.parseLong(tweet[0]);                                              //Converts the epoch_time field into a long variable
              LocalDateTime date = LocalDateTime.ofEpochSecond(EpochTime/1000,0,ZoneOffset.UTC);    //Converts the milliseconds into a time and date in UTC time zone
            	int hour = date.getHour();                                                             //Extracts the hour
            	data.set(hour);
            	}catch(Exception e) {}


    	}
    	context.write(data, one);                                                                    //Send hour and IntWritablevalue of 1 to reducer
    }
}
