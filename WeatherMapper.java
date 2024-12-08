import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {
    private boolean isFirstLine = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isFirstLine) {
            isFirstLine = false;
            return; // Bỏ qua dòng đầu tiên
        }

        String[] fields = value.toString().split(",");
        if (fields.length == 10) {
            String province = fields[0];
            String date = fields[9];
            String yearMonth = date.substring(0, 7); // Lấy năm-tháng
            String year = date.substring(0, 4); // Lấy năm
            double maxTemp = Integer.parseInt(fields[1]);
            double minTemp = Integer.parseInt(fields[2]);
            double wind = Integer.parseInt(fields[3]);
            double rain = Double.parseDouble(fields[5]);
            double humidity = Integer.parseInt(fields[6]);
            double cloud = Integer.parseInt(fields[7]);

            context.write(new Text(province + "," + yearMonth), new Text("TMAX," + maxTemp));
            context.write(new Text(province + "," + yearMonth), new Text("TMIN," + minTemp));
            context.write(new Text(province + "," + yearMonth), new Text("WIND," + wind));
            context.write(new Text(province + "," + yearMonth), new Text("RAIN," + rain));
            context.write(new Text(province + "," + yearMonth), new Text("HUMIDITY," + humidity));
            context.write(new Text(province + "," + yearMonth), new Text("TAVG_MONTH," + ((maxTemp + minTemp)/2)));
            
            context.write(new Text(province + "," + year), new Text("TMAX_YEAR," + maxTemp));
            context.write(new Text(province + "," + year), new Text("TMIN_YEAR," + minTemp));
            context.write(new Text(province + "," + year), new Text("TAVG_YEAR," + ((maxTemp + minTemp)/2)));
            context.write(new Text(province + "," + year), new Text("WIND_YEAR," + wind));
            context.write(new Text(province + "," + year), new Text("TOTAL_RAIN_YEAR," + rain));
            context.write(new Text(province + "," + year), new Text("HUMIDITY_YEAR," + humidity));
            context.write(new Text(province + "," + year), new Text("CLOUD_YEAR," + cloud));            
        }
    }
}