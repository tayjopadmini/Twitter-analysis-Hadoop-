import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Date;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class HashMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String tweet[]=value.toString().split(";");                                                    //converts the input line into a String and splits it on ;. These values are stored in tweet
    	
    	if (tweet.length==4)                                                                          //Data cleaning. checks if the srtring has 4 parts after splitting on a ;
    	{
    		try {
            	long EpochTime=Long.parseLong(tweet[0]);                                              //Converts the epoch_time field into a long variable
              LocalDateTime date = LocalDateTime.ofEpochSecond(EpochTime/1000,0,ZoneOffset.UTC);    //Converts the milliseconds into a time and date in UTC time zone
            	int hour = date.getHour();                                                            //Extracts the hour
            	if (hour==01)                                                                         //Checks if the hour is 1:00 am in UTC(most popular hour)
            	{
            	   String [] fields = tweet[2].split(" ");                                            //Splits the tweet message on space character and stores it in an array 'fields'

                  for(int i=0 ; i <fields.length ; i++)                                             //for every word in the fields array
                  {
                    if(fields[i].charAt(0)=='#')                                                    //checks if the first character is a '#'
                      {
                        String result = fields[i].toLowerCase();                                     //converts the hash tag word to lower case
                        data.set(result);
                        context.write(data, one);                                                     //send the hash tag word and IntWritable value of one to reducer
                      }
                  }



            	}

            	}catch(Exception e) {}

    }

    }
}
