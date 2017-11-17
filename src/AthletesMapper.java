import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Hashtable;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AthletesMapper extends Mapper<Object, Text, Text, IntWritable> {

  private Set<String> athletesNames;
  private Hashtable<String, Pattern> athletesPatterns;
  private Matcher matcher;

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] data = value.toString().toLowerCase().split(";");

    if(data.length == 4){
      Integer len = data[2].length();
      if(len <= 140 && data[0].length() <= 13){ // some epochs are invalid
        for(String name : athletesNames) {
          matcher = athletesPatterns.get(name).matcher(data[2]);
          Integer count = 0;
          while(matcher.find())
            count++;
          if(count > 0)
            context.write(new Text(name), new IntWritable(count));
        }
      }
    }
  }


  @Override
	protected void setup(Context context) throws IOException, InterruptedException {

    athletesPatterns = new Hashtable<String, Pattern>();

		// getting the medalist cache file
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			// discard header
			br.readLine();

			while ((line = br.readLine()) != null) {
				String[] fields = line.split(",");
				// Fields: Name 1
				if (fields.length == 11){
          String name =  fields[1].toLowerCase();
          athletesPatterns.put(name, Pattern.compile(name));
        }
			}
			br.close();
		} catch (IOException e1) {
		}

    athletesNames = athletesPatterns.keySet();


		super.setup(context);
	}
}
