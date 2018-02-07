import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;



public class AthletesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {

    		int sum=0;
    		for(IntWritable value: values){
    			sum=sum+value.get();           //Recieves athelete name and for each name sums the 1's
    		}
    		result.set(sum);                 //Sets the final sum as the result

        context.write(key,result);       //outputs the athlete name and its count
        }


}
