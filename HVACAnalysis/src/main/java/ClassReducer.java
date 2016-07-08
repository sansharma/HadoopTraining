import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * Created by l on 7/7/16.
 */
public class ClassReducer extends Reducer<IntWritable,Text,NullWritable,Text>{

    private Splitter splitter;
    private Joiner joiner;
    boolean lineone;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        splitter = Splitter.on(",").trimResults();
        joiner = Joiner.on(",");
        lineone = true;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double expectedSum=0.0,actualSum=0.0;
        int totalCount = 0;
        String modelNum = "";
        for(Text val:values){
            List<String> rows = Lists.newArrayList(splitter.split(val.toString()));
            expectedSum+= Double.parseDouble(rows.get(rows.size()-2));
            actualSum+= Double.parseDouble(rows.get(rows.size()-1)+"0");
            modelNumber = rows.get(1);
            totalCount++;
        }

        if(lineone){
            context.write(null,new Text("Building Id,Building Id,Model Number,Expected,Actual"));
            lineone = false;
        }

        String mergedVal = String.valueOf(key)+","+modelNum+","+(expectedSum/totalCount)+","+(actualSum/totalCount);

        context.write(null,new Text(mergedVal));

    }
}
