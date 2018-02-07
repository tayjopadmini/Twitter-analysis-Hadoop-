import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LengthMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);

    private Text data = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tweet;
        String message;
        int len;
        tweet=value.toString().split(";");                 //converts the input line into a String and splits it on ;. These values are stored in tweet
        if(tweet.length==4)                                //Data cleaning. checks if the srtring has 4 parts after splitting on a ;
        {
          message = tweet[2];                              //stores the actual tweet message in a string variable called message
          len= message.length();                           //calculates the length of the message and stores it in len
          if (len>=1 && len<=140)                          //Data filtering. Only takes the message if it has between 1 and 140 characters
          {
            for (int i=5; i<=140; i=i+5)                    //Running a loop with counter i that begins with upper bound of bins(beginning with 5)
            {
              if(len<=i)                                    //Checks if the length of the message is less than the upper bound
              {
                  String print = ((i-4)+"-"+(i));           //Calculates the bin
                  context.write(new Text(print),one);       //Sending the bins and the IntWritable value of 1 to reducer
              }
            }
          }


      }
    }
}
