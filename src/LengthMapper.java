import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class LengthMapper extends Mapper<Object, Text, Text, IntWritable> {

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] data = value.toString().split(";");

    if(data.length == 4){
      Integer len = data[2].length();
      if(len <= 140){
        Integer bottomRange = Double.valueOf(Math.floor((len-1)/5)).intValue() * 5 + 1;
        String range = Integer.toString(bottomRange) + " - " + Integer.toString(bottomRange + 4);
        context.write(new Text(range), new IntWritable(1));
      }
    }
  }
}
