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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SportsReplJoinMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Hashtable<String, String> atheletesInfo;

	private IntWritable one = new IntWritable();
	private Text data = new Text();
	String[] sportsmen;
	String athelete;
	int len;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//here the input file is the sorted output file from the previous activity and contains the athlete name and number of mentions
		sportsmen=value.toString().split(",");													//converts the input line into a String and splits it on ,. These values are stored in sportsmen
		if(sportsmen.length==2)																					//Data cleaning. checks if the srtring has 4 parts after splitting on a ;
		{

			for(String name: atheletesInfo.keySet()){											//for every key in the atheletesInfohash table

				if(sportsmen[0].toLowerCase().contains(name.toLowerCase()))	//if the sportsmen array has the name contained in the key of the hash table
				{
					data.set(atheletesInfo.get(name));												//gets the sport of the athelete
					one.set(Integer.parseInt(sportsmen[1]));									//sets the variable one with the number of mentions of the sportsman
					context.write(data, one);																	//send the sport along with the number of mentions of the respective athletes to the reducers
				}

			}



		}
	}




	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		atheletesInfo = new Hashtable<String, String>();									//creates a new hashtable with 2 string values


		URI fileUri = context.getCacheFiles()[0];													// We know there is only one cache file, so we only retrieve that URI

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {

			br.readLine();

			while ((line = br.readLine()) != null) {


				String[] fields = line.split(",");							//splits the secondary dataset on ',' and stores it in string array called fields

				if (fields.length == 11)												//checks if the fields array has 11 parts after splitting
					atheletesInfo.put(fields[1], fields[7]);			//stores the name of the athlete and the sport into the hashtable
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}

}
