import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AtheletesReplJoinMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Hashtable<String, String> atheletesInfo;

	private IntWritable one = new IntWritable(1);
	private Text data = new Text();
	String[] tweet;
	String message;
	int len;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


		tweet=value.toString().split(";");																//converts the input line into a String and splits it on ;. These values are stored in tweet
		if(tweet.length==4)																								//Data cleaning. checks if the srtring has 4 parts after splitting on a ;
		{
			message = tweet[2];																							//stores the actual tweetmessage in a variable called message
			for(String name: atheletesInfo.keySet()){												//for every key in the atheletesInfohash table

				if(message.toLowerCase().contains(name.toLowerCase()))				//if the tweet contains the name in the key of the hashtable
				{
					data.set(name);
					context.write(data, one);																		//pass the name and IntWritable value of 1 to reducer
				}
			}



		}
	}





	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		atheletesInfo = new Hashtable<String, String>();										//creates a new hashtable with 2 string values

		URI fileUri = context.getCacheFiles()[0];														// We know there is only one cache file, so we only retrieve that URI

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {

			br.readLine();

			while ((line = br.readLine()) != null) {

				String[] fields = line.split(",");									//splits the secondary dataset on ',' and stores it in string array called fields
				if (fields.length == 11)														//checks if the fields array has 11 parts after splitting
					atheletesInfo.put(fields[1], fields[7]);					//stores the name of the athlete and the sport into the hashtable
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}

}
