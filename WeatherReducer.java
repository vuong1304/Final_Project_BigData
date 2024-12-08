import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Double> provinceYearlyAvgTemp = new HashMap<>();
    private Map<String, Double> provinceYearlyAvgRain = new HashMap<>();
    private Map<String, Double> provinceYearlyTotalRain = new HashMap<>();
    private Map<String, TreeMap<Integer, Double>> tempTrend = new HashMap<>();
    private Map<String, TreeMap<Integer, Double>> rainTrend = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] keyParts = key.toString().split(",");
        String province = keyParts[0];
        String yearMonthOrYear = keyParts[1];

        Map<String, Double> tempSum = new HashMap<>();
        Map<String, Integer> tempCount = new HashMap<>();
        Map<String, Double> windSum = new HashMap<>();
        Map<String, Integer> windCount = new HashMap<>();
        Map<String, Double> rainSum = new HashMap<>();
        Map<String, Integer> rainCount = new HashMap<>();
        Map<String, Double> humiditySum = new HashMap<>();
        Map<String, Integer> humidityCount = new HashMap<>();
        Map<String, Double> cloudSum = new HashMap<>();
        Map<String, Integer> cloudCount = new HashMap<>();
        
        
        

        double maxTempYear = Integer.MIN_VALUE;
        double minTempYear = Integer.MAX_VALUE;
        double maxHumidityYear = Integer.MIN_VALUE;
        double minHumidityYear = Integer.MAX_VALUE;
        
        double sumTemp = 0, sumHumidity = 0, sumRain = 0, sumCloud = 0;
        double sumTempSquared = 0, sumHumiditySquared = 0, sumRainSquared = 0, sumCloudSquared = 0;
        double sumTempHumidity = 0, sumTempRain = 0, sumTempCloud = 0, sumRainCloud = 0, sumRainHumidity = 0;
        int count = 0;
        double currentTemp = 0;
        double currentRain = 0;

        for (Text val : values) {
            String[] valParts = val.toString().split(",");
            String type = valParts[0];
            double value = Double.parseDouble(valParts[1]);


            switch (type) {
                case "TMAX":
                    tempSum.put(type, tempSum.getOrDefault(type, (double) 0) + value);
                    tempCount.put(type, tempCount.getOrDefault(type, 0) + 1);
                    break;
                case "TMIN":
                    tempSum.put(type, tempSum.getOrDefault(type, (double) 0) + value);
                    tempCount.put(type, tempCount.getOrDefault(type, 0) + 1);
                    break;
                case "TAVG_MONTH":
                	tempSum.put(type, tempSum.getOrDefault(type, (double) 0) + value);
                	tempCount.put(type, tempCount.getOrDefault(type, 0) + 1);
                	break;
                case "WIND":
                    windSum.put(type, windSum.getOrDefault(type, (double) 0) + value);
                    windCount.put(type, windCount.getOrDefault(type, 0) + 1);
                    break;
                case "RAIN":
                	rainSum.put(type, rainSum.getOrDefault(type, 0.0) + value);
                	rainCount.put(type, rainCount.getOrDefault(type, 0) + 1);
                    break;
                case "HUMIDITY":
                    humiditySum.put(type, humiditySum.getOrDefault(type, (double) 0) + value);
                    humidityCount.put(type, humidityCount.getOrDefault(type, 0) + 1);
                    break;
                case "TOTAL_RAIN_YEAR":
                	rainSum.put(type, rainSum.getOrDefault(type, 0.0) + value);
                	rainCount.put(type, rainCount.getOrDefault(type, 0) + 1);
             
                	currentRain = value;
                	sumRain += currentRain;
                	sumRainSquared += value * value;
                	sumTempRain += currentTemp * value;
                	break;
                case "TMAX_YEAR":
                    if ( value > maxTempYear) {
                        maxTempYear = value;
                    }
                    break;
                case "TMIN_YEAR":
                    if ( value < minTempYear) {
                        minTempYear = (int) value;
                    }
                    break;
                case "TAVG_YEAR":
                	tempSum.put(type, tempSum.getOrDefault(type, (double) 0) + value);
                	tempCount.put(type, tempCount.getOrDefault(type, 0) + 1);
                	
                	currentTemp = value;
                	sumTemp += currentTemp;
                	sumTempSquared += value * value;
                	break;
                case "HUMIDITY_YEAR":
                    if ( value > maxHumidityYear) {
                        maxHumidityYear = value;
                    }
                    if ( value < minHumidityYear) {
                        minHumidityYear = value;
                    }
                    humiditySum.put(type, humiditySum.getOrDefault(type, (double) 0) + value);
                    humidityCount.put(type, humidityCount.getOrDefault(type, 0) + 1);
                    
                    sumHumidity += value;
                    sumHumiditySquared += value * value;
                    sumTempHumidity += currentTemp * value;
                    sumRainHumidity += currentRain * value;
                    break;
                case "CLOUD_YEAR":
                	cloudSum.put(type, cloudSum.getOrDefault(type, (double) 0) + value);
                	cloudCount.put(type, cloudCount.getOrDefault(type, 0) + 1);
                	
                	sumCloud += value;
                	sumCloudSquared += value * value;
                	sumTempCloud += currentTemp * value;
                	sumRainCloud += currentRain * value;
                	break;
                case "WIND_YEAR":
                    windSum.put(type, windSum.getOrDefault(type, (double) 0) + value);
                    windCount.put(type, windCount.getOrDefault(type, 0) + 1);
            }
            count++;
        }

        if (yearMonthOrYear.length() == 7) { // Year-Month
            double avgMaxTemp = tempSum.getOrDefault("TMAX", (double) 0) / tempCount.getOrDefault("TMAX", 1);
            double avgMinTemp = tempSum.getOrDefault("TMIN", (double) 0) / tempCount.getOrDefault("TMIN", 1);
            double avgTempMonth = tempSum.getOrDefault("TAVG_MONTH", (double) 0) / tempCount.getOrDefault("TAVG_MONTH", 1);
            double avgWindSpeed = windSum.getOrDefault("WIND", (double) 0) / windCount.getOrDefault("WIND", 1);
            double avgHumidity = humiditySum.getOrDefault("HUMIDITY", (double) 0) / humidityCount.getOrDefault("HUMIDITY", 1);
            double avgRainMonth = rainSum.getOrDefault("RAIN", (double) 0) / rainCount.getOrDefault("RAIN", 1); 

            context.write(new Text("Average MaxTemperature of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                          new Text(String.format("%.2f", avgMaxTemp)));
            context.write(new Text("Average MinTemperature of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                          new Text(String.format("%.2f", avgMinTemp)));
            context.write(new Text("Average Temperature of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                    	  new Text(String.format("%.2f", avgTempMonth)));
            context.write(new Text("Average Wind speed of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
              	  		  new Text(String.format("%.2f", avgWindSpeed)));
            context.write(new Text("Average Humidity of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                          new Text(String.format("%.2f", avgHumidity)));
            context.write(new Text("Average Rain of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                          new Text(String.format("%.2f", avgRainMonth)));
        } else { // Year
        	double avgTempYear = tempSum.getOrDefault("TAVG_YEAR", (double) 0) / tempCount.getOrDefault("TAVG_YEAR", 1);
        	double avgWindSpeedYear = windSum.getOrDefault("WIND_YEAR", (double) 0) / windCount.getOrDefault("WIND_YEAR", 1);
        	double totalRain = rainSum.getOrDefault("TOTAL_RAIN_YEAR", (double) 0); 
        	double avgHumidityYear = humiditySum.getOrDefault("HUMIDITY_YEAR", (double) 0) / humidityCount.getOrDefault("HUMIDITY_YEAR", 1);
        	int rainDays = rainCount.getOrDefault("TOTAL_RAIN_YEAR", 1);
        	double avgRainYear = totalRain / rainDays;
        	
        	
        	int year = Integer.parseInt(yearMonthOrYear);
        	
        	double corrTempHumidity = calculateCorrelation(count, sumTemp, sumHumidity, sumTempSquared, sumHumiditySquared, sumTempHumidity);
            double corrTempRain = calculateCorrelation(count, sumTemp, sumRain, sumTempSquared, sumRainSquared, sumTempRain);
            double corrTempCloud = calculateCorrelation(count, sumTemp, sumCloud, sumTempSquared, sumCloudSquared, sumTempCloud);
            double corrRainCloud = calculateCorrelation(count, sumRain, sumCloud, sumRainSquared, sumCloudSquared, sumRainCloud);
            double corrRainHumidity = calculateCorrelation(count, sumRain, sumHumidity, sumRainSquared, sumHumiditySquared, sumRainHumidity);
        	
        	
        	context.write(new Text("Total Rain of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
        	              new Text(String.format("%.2f", totalRain)));
            context.write(new Text("Max Temperature of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                          new Text(String.format("%.2f", maxTempYear)));
            context.write(new Text("Min Temperature of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                          new Text(String.format("%.2f", minTempYear)));
            context.write(new Text("Average Temperature of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                          new Text(String.format("%.2f", avgTempYear)));
            context.write(new Text("Max Humidity of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                          new Text(String.format("%.2f", maxHumidityYear)));
            context.write(new Text("Min Humidity of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                          new Text(String.format("%.2f", minHumidityYear)));
            context.write(new Text("Average Humidity of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                    	  new Text(String.format("%.2f", avgHumidityYear)));
            context.write(new Text("Average Humidity of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                          new Text(String.format("%.2f", avgTempYear)));
            context.write(new Text("Average Wind speed of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                    	  new Text(String.format("%.2f", avgWindSpeedYear)));
            
            
            context.write(new Text("Correlation between Temperature and Humidity for \"" + province + "," + yearMonthOrYear + "\":"), 
                    	  new Text(String.format("%.4f", corrTempHumidity)));
            context.write(new Text("Correlation between Temperature and Rain for \"" + province + "," + yearMonthOrYear + "\":"), 
                    	  new Text(String.format("%.4f", corrTempRain)));
            context.write(new Text("Correlation between Temperature and Cloud for \"" + province + "," + yearMonthOrYear + "\":"), 
            			  new Text(String.format("%.4f", corrTempCloud)));
            context.write(new Text("Correlation between Rain and Cloud for \"" + province + "," + yearMonthOrYear + "\":"), 
                    	  new Text(String.format("%.4f", corrRainCloud)));
            context.write(new Text("Correlation between Rain and Humidity for \"" + province + "," + yearMonthOrYear + "\":"), 
                    	  new Text(String.format("%.4f", corrRainHumidity)));
            
            
            provinceYearlyAvgTemp.put(province + "," + yearMonthOrYear, avgTempYear);
            provinceYearlyAvgRain.put(province + "," + yearMonthOrYear, avgRainYear);
            provinceYearlyTotalRain.put(province + "," + yearMonthOrYear, totalRain);
            tempTrend.putIfAbsent(province, new TreeMap<>());
            tempTrend.get(province).put(year, avgTempYear);
            
            rainTrend.putIfAbsent(province, new TreeMap<>());
            rainTrend.get(province).put(year, avgRainYear);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // So sánh giữa các tỉnh
        String maxTempProvince = null;
        double maxTemp = Double.MIN_VALUE;
        String minTempProvince = null;
        double minTemp = Double.MAX_VALUE;
        String maxRainTotalProvince = null;
        double maxRainTotal = Double.MIN_VALUE;
        String minRainTotalProvince = null;
        double minRainTotal = Double.MAX_VALUE;
        String maxRainYearProvince = null;
        double maxRainYear = Double.MIN_VALUE;
        


        for (Map.Entry<String, Double> entry : provinceYearlyAvgTemp.entrySet()) {
            if (entry.getValue() > maxTemp) {
                maxTemp = entry.getValue();
                maxTempProvince = entry.getKey();
            }
        }
        
        for (Map.Entry<String, Double> entry : provinceYearlyAvgTemp.entrySet()) {
            if (entry.getValue() < minTemp) {
            	minTemp = entry.getValue();
                minTempProvince = entry.getKey();
            }
        }

        for (Map.Entry<String, Double> entry : provinceYearlyTotalRain.entrySet()) {
            if (entry.getValue() > maxRainTotal) {
            	maxRainTotal = entry.getValue();
                maxRainTotalProvince = entry.getKey();
            }
        }
        
        for (Map.Entry<String, Double> entry : provinceYearlyTotalRain.entrySet()) {
            if (entry.getValue() > minRainTotal) {
            	minRainTotal = entry.getValue();
            	minRainTotalProvince = entry.getKey();
            }
        }
        
        for (Map.Entry<String, Double> entry : provinceYearlyAvgRain.entrySet()) {
            if (entry.getValue() > maxRainYear) {
            	maxRainYear = entry.getValue();
            	maxRainYearProvince = entry.getKey();
            }
        }
        
        for (String province : tempTrend.keySet()) {
            TreeMap<Integer, Double> tempData = tempTrend.get(province);
            TreeMap<Integer, Double> rainData = rainTrend.get(province);
            
            double tempSlope = calculateTrend(tempData);
            String tempTrendResult = tempSlope > 0 ? "Increasing" : (tempSlope < 0 ? "Decreasing" : "Stable");
            
            double rainSlope = calculateTrend(rainData);
            String rainTrendResult = rainSlope > 0 ? "Increasing" : (rainSlope < 0 ? "Decreasing" : "Stable");
            
            context.write(new Text("Temperature Trend for province \"" + province + "\":"), new Text(tempTrendResult));
            context.write(new Text("Rain Trend for province \"" + province + "\":"), new Text(rainTrendResult));
        }
        


            context.write(new Text("Province with highest yearly average temperature:"), new Text(maxTempProvince + " with " + maxTemp));
            context.write(new Text("Province with lowest yearly average temperature:"), new Text(minTempProvince + " with " + minTemp));
            context.write(new Text("Province with highest yearly total rain:"), new Text(maxRainTotalProvince + " with " + maxRainTotal));
            context.write(new Text("Province with lowest yearly total rain:"), new Text(minRainTotalProvince + " with " + minRainTotal));
            context.write(new Text("Province with highest yearly average rain:"), new Text(maxRainYearProvince + " with " + maxRainYear));
    }
    
    private double calculateTrend(TreeMap<Integer, Double> data) {
    	//Tính độ dốc (slope) để xác định xu hướng.
    	
        int n = data.size();

        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;

        for (Map.Entry<Integer, Double> entry : data.entrySet()) {
            int year = entry.getKey();
            double value = entry.getValue();

            sumX += year;
            sumY += value;
            sumXY += year * value;
            sumX2 += year * year;
        }

        return (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
    }
    
    private double calculateCorrelation(int n, double sumX, double sumY, double sumX2, double sumY2, double sumXY) {
    	// Tính độ chỉ số tương quan Pearson
        double numerator = n * sumXY - sumX * sumY;
        double denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));
        double result = numerator / denominator;
        return Math.max(-1.0, Math.min(1.0, result));
    }
    
}