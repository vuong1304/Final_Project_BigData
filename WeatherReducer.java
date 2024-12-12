import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Double> provinceYearlyAvgTemp = new HashMap<>();
    private Map<String, Double> provinceYearlyAvgRain = new HashMap<>();
    private Map<String, Double> provinceYearlyAvgHumidity = new HashMap<>();
    private Map<String, Double> provinceYearlyTotalRain = new HashMap<>();
    private Map<String, TreeMap<Integer, Double>> tempTrend = new HashMap<>();
    private Map<String, TreeMap<Integer, Double>> rainTrend = new HashMap<>();
    private Map<String, TreeMap<Integer, Double>> humidityTrend = new HashMap<>();
    private Map<String, TreeMap<Integer, Double>> heatIndexTrend = new HashMap<>();
    
    // Thêm các Map để lưu trữ dữ liệu theo tháng
    private Map<String, TreeMap<String, Double>> monthlyTempData = new HashMap<>();
    private Map<String, TreeMap<String, Double>> monthlyRainData = new HashMap<>();
    private Map<String, TreeMap<String, Double>> monthlyHumidityData = new HashMap<>();

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
        double currentHumidity = 0;

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
                    
                    currentHumidity = value;
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

            // Lưu trữ dữ liệu theo tháng
            monthlyTempData.putIfAbsent(province, new TreeMap<>());
            monthlyTempData.get(province).put(yearMonthOrYear, avgTempMonth);
            monthlyRainData.putIfAbsent(province, new TreeMap<>());
            monthlyRainData.get(province).put(yearMonthOrYear, avgRainMonth);
            monthlyHumidityData.putIfAbsent(province, new TreeMap<>());
            monthlyHumidityData.get(province).put(yearMonthOrYear, avgHumidity);
            
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
        	double heatIndex = calculateHeatIndex(avgTempYear, avgHumidityYear);
        	
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
            context.write(new Text("Average Temperature of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                          new Text(String.format("%.2f", avgTempYear)));
            context.write(new Text("Average Wind speed of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
                    	  new Text(String.format("%.2f", avgWindSpeedYear)));
            context.write(new Text("Heat Index of \"" + province + "\" in \"" + yearMonthOrYear + "\":"), 
            		new Text(String.format("%.2f", heatIndex)));
            
            
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
            provinceYearlyAvgHumidity.put(province + "," + yearMonthOrYear, avgHumidityYear);
            provinceYearlyAvgRain.put(province + "," + yearMonthOrYear, avgRainYear);
            provinceYearlyTotalRain.put(province + "," + yearMonthOrYear, totalRain);
            tempTrend.putIfAbsent(province, new TreeMap<>());
            tempTrend.get(province).put(year, avgTempYear);
            
            rainTrend.putIfAbsent(province, new TreeMap<>());
            rainTrend.get(province).put(year, avgRainYear);
            
            humidityTrend.putIfAbsent(province, new TreeMap<>());
            humidityTrend.get(province).put(year, avgHumidityYear);
            
            heatIndexTrend.putIfAbsent(province, new TreeMap<>());
            heatIndexTrend.get(province).put(year, heatIndex);
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
        String maxHumidityProvince = null;
        double maxHumidity = Double.MIN_VALUE;
        String minHumidityProvince = null;
        double minHumidity = Double.MAX_VALUE;
        

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
        
        for (Map.Entry<String, Double> entry : provinceYearlyAvgHumidity.entrySet()) {
            if (entry.getValue() > maxHumidity) {
            	maxHumidity = entry.getValue();
            	maxHumidityProvince = entry.getKey();
            }
        }
        
        for (Map.Entry<String, Double> entry : provinceYearlyAvgHumidity.entrySet()) {
            if (entry.getValue() < minHumidity) {
            	minHumidity = entry.getValue();
            	minHumidityProvince = entry.getKey();
            }
        }

        for (Map.Entry<String, Double> entry : provinceYearlyTotalRain.entrySet()) {
            if (entry.getValue() > maxRainTotal) {
            	maxRainTotal = entry.getValue();
                maxRainTotalProvince = entry.getKey();
            }
        }
        
        for (Map.Entry<String, Double> entry : provinceYearlyTotalRain.entrySet()) {
            if (entry.getValue() < minRainTotal) {
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
            TreeMap<Integer, Double> humidityData = humidityTrend.get(province);
            TreeMap<Integer, Double> heatIndexData = heatIndexTrend.get(province);
            
            // Mann-Kendall Test
            double tempMannKendall = calculateMannKendall(tempData);
            double rainMannKendall = calculateMannKendall(rainData);
            double humidityMannKendall = calculateMannKendall(humidityData);
            double heatIndexMannKendall = calculateMannKendall(heatIndexData);
            
            // Sen's Slope
            double tempSlope = calculateSensSlope(tempData);
            double rainSlope = calculateSensSlope(rainData);
            double humiditySlope = calculateSensSlope(humidityData);
            double heatIndexSlope = calculateSensSlope(heatIndexData);
            
            String tempTrendResult = tempSlope > 0 ? "Increasing" : (tempSlope < 0 ? "Decreasing" : "Stable");
            String rainTrendResult = rainSlope > 0 ? "Increasing" : (rainSlope < 0 ? "Decreasing" : "Stable");
            String humidityTrendResult = humiditySlope > 0 ? "Increasing" : (humiditySlope < 0 ? "Decreasing" : "Stable");
            String heatIndexTrendResult = heatIndexSlope > 0 ? "Increasing" : (heatIndexSlope < 0 ? "Decreasing" : "Stable");
            
            context.write(new Text("Temperature Trend for province \"" + province + "\":"), new Text(tempTrendResult + " (Mann-Kendall: " + tempMannKendall + ", Sen's Slope: " + tempSlope + ")"));
            context.write(new Text("Rain Trend for province \"" + province + "\":"), new Text(rainTrendResult + " (Mann-Kendall: " + rainMannKendall + ", Sen's Slope: " + rainSlope + ")"));
            context.write(new Text("Humidity Trend for province \"" + province + "\":"), new Text(humidityTrendResult + " (Mann-Kendall: " + humidityMannKendall + ", Sen's Slope: " + humiditySlope + ")"));
            context.write(new Text("Heat Index Trend for province \"" + province + "\":"), new Text(heatIndexTrendResult + " (Mann-Kendall: " + heatIndexMannKendall + ", Sen's Slope: " + heatIndexSlope + ")"));
            
            // Phân tích chu kỳ
            performCyclicalAnalysis(tempData, "Temperature", province, context);
            performCyclicalAnalysis(rainData, "Rain", province, context);
            performCyclicalAnalysis(humidityData, "Humidity", province, context);
            performCyclicalAnalysis(heatIndexData, "Heat Index", province, context);
            
            // Tính Seasonal Average cho Temperature, Rain, Humidity và Heat Index
            calculateSeasonalAverage(monthlyTempData.get(province), "Temperature", province, context);
            calculateSeasonalAverage(monthlyRainData.get(province), "Rain", province, context);
            calculateSeasonalAverage(monthlyHumidityData.get(province), "Humidity", province, context);
            calculateSeasonalAverageForHeatIndex(monthlyTempData.get(province), monthlyHumidityData.get(province), "Heat Index", province, context);
        }
        

            context.write(new Text("Province with highest yearly average temperature:"), new Text(maxTempProvince + " with " + maxTemp));
            context.write(new Text("Province with lowest yearly average temperature:"), new Text(minTempProvince + " with " + minTemp));
            context.write(new Text("Province with highest yearly average humidity:"), new Text(maxHumidityProvince + " with " + maxHumidity));
            context.write(new Text("Province with lowest yearly average humidity:"), new Text(minHumidityProvince + " with " + minHumidity));
            context.write(new Text("Province with highest yearly total rain:"), new Text(maxRainTotalProvince + " with " + maxRainTotal));
            context.write(new Text("Province with lowest yearly total rain:"), new Text(minRainTotalProvince + " with " + minRainTotal));
            context.write(new Text("Province with highest daily average rain:"), new Text(maxRainYearProvince + " with " + maxRainYear));
    }
    
    private double calculateCorrelation(int n, double sumX, double sumY, double sumX2, double sumY2, double sumXY) {
    	// Tính độ chỉ số tương quan Pearson
        double numerator = n * sumXY - sumX * sumY;
        double denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));
        double result = numerator / denominator;
        return Math.max(-1.0, Math.min(1.0, result));
    }
    
 // Phương thức tính toán Heat Index
    private double calculateHeatIndex(double temperature, double humidity) {
        // Chuyển đổi nhiệt độ sang độ F
        double tempF = (temperature * 9 / 5) + 32;

        // Tính toán Heat Index
        double heatIndex = -42.379 + 2.04901523 * tempF + 10.14333127 * humidity
                - 0.22475541 * tempF * humidity - 0.00683783 * tempF * tempF
                - 0.05481717 * humidity * humidity + 0.00122874 * tempF * tempF * humidity
                + 0.00085282 * tempF * humidity * humidity - 0.00000199 * tempF * tempF * humidity * humidity;
        
        if (humidity < 13 && tempF >= 80 && tempF <= 112) {
        	heatIndex -= ((13 - humidity) / 4) * Math.sqrt((17 - Math.abs(tempF - 95)) / 17);
        }
        else if (humidity > 85 && tempF >= 80 && tempF <= 87) {
        	heatIndex += ((humidity - 85) / 10) * ((87 - tempF) / 5);
        }
        
        if (heatIndex < 80) {
        	heatIndex = 0.5 * (tempF + 61 + (tempF - 68) * 1.2 + humidity * 0.094);
        }

        // Chuyển đổi Heat Index sang độ C
        return (heatIndex - 32) * 5 / 9;
    }

    // Phương thức thực hiện kiểm định Mann-Kendall
    private double calculateMannKendall(TreeMap<Integer, Double> data) {
        int n = data.size();


        double[] values = data.values().stream().mapToDouble(Double::doubleValue).toArray();
        double s = 0;

        for (int i = 0; i < n - 1; i++) {
            for (int j = i + 1; j < n; j++) {
                s += Math.signum(values[j] - values[i]);
            }
        }

        double varS = (n * (n - 1) * (2 * n + 5)) / 18.0;
        double z;

        if (s > 0) {
            z = (s - 1) / Math.sqrt(varS);
        } else if (s < 0) {
            z = (s + 1) / Math.sqrt(varS);
        } else {
            z = 0;
        }

        return z; 
    }

    // Phương thức tính toán Sen's Slope
    private double calculateSensSlope(TreeMap<Integer, Double> data) {
    	if (data.size() < 2) {
            return 0;
        }
    	
        SimpleRegression regression = new SimpleRegression();
        for (Map.Entry<Integer, Double> entry : data.entrySet()) {
            regression.addData(entry.getKey(), entry.getValue());
        }

        return regression.getSlope();
    }

    // Phương thức thực hiện phân tích chu kỳ
    private void performCyclicalAnalysis(TreeMap<Integer, Double> data, String dataType, String province, Context context) throws IOException, InterruptedException {
    	if (data.size() < 2) {
        	context.write(new Text("Not enough data for Cyclical Analysis of " + dataType + " for province \"" + province + "\""), new Text(""));
            return;
        }

        // Tính trung bình theo năm
        Map<Integer, Double> yearlySum = new HashMap<>();
        Map<Integer, Integer> yearlyCount = new HashMap<>();
        for (Map.Entry<Integer, Double> entry : data.entrySet()) {
            int year = entry.getKey();
            double value = entry.getValue();
            yearlySum.put(year, yearlySum.getOrDefault(year, 0.0) + value);
            yearlyCount.put(year, yearlyCount.getOrDefault(year, 0) + 1);
        }
        
        Map<Integer, Double> yearlyAvg = new HashMap<>();
        for (int year : yearlySum.keySet()) {
        	yearlyAvg.put(year, yearlySum.get(year) / yearlyCount.get(year));
        }

        // Tính trung bình theo chu kỳ năm
        double cycle5Sum = 0, cycle10Sum = 0;
        int cycle5Count = 0, cycle10Count = 0;
        for (Map.Entry<Integer, Double> entry : yearlyAvg.entrySet()) {
            int year = entry.getKey();
            double value = entry.getValue();

            if (year >= 2009 && year <= 2013) {
                cycle5Sum += value;
                cycle5Count++;
            }
            if (year >= 2009 && year <= 2018) {
                cycle10Sum += value;
                cycle10Count++;
            }
        }

        if (cycle5Count > 0) {
            double cycle5Avg = cycle5Sum / cycle5Count;
            context.write(new Text("5-Year Cycle Average " + dataType + " for province \"" + province + "\" (2009-2013):"), new Text(String.format("%.2f", cycle5Avg)));
        }
        if (cycle10Count > 0) {
            double cycle10Avg = cycle10Sum / cycle10Count;
            context.write(new Text("10-Year Cycle Average " + dataType + " for province \"" + province + "\" (2009-2018):"), new Text(String.format("%.2f", cycle10Avg)));
        }
    }
    
    // Tính Seasonal Average 
    private void calculateSeasonalAverage(TreeMap<String, Double> monthlyData, String dataType, String province, Context context) throws IOException, InterruptedException {
    	if (monthlyData == null || monthlyData.size() == 0) {
            context.write(new Text("Insufficient data for Seasonal Average of " + dataType + " for province \"" + province + "\""), new Text(""));
            return;
        }
    	
    	Map<Integer, Double> seasonalSum = new HashMap<>();
        Map<Integer, Integer> seasonalCount = new HashMap<>();

        // Xác định mùa dựa vào tháng
        for (Map.Entry<String, Double> entry : monthlyData.entrySet()) {
            String yearMonth = entry.getKey();
            double value = entry.getValue();
            int month = Integer.parseInt(yearMonth.substring(5, 7));
            int season = getSeasonFromMonth(month); // 1: Xuân, 2: Hạ, 3: Thu, 4: Đông

            seasonalSum.put(season, seasonalSum.getOrDefault(season, 0.0) + value);
            seasonalCount.put(season, seasonalCount.getOrDefault(season, 0) + 1);
        }

        // Tính trung bình và ghi kết quả
        for (int season : seasonalSum.keySet()) {
        	if (seasonalCount.get(season) > 0) {
	            double seasonalAvg = seasonalSum.get(season) / seasonalCount.get(season);
	            String seasonName = getSeasonName(season);
	            context.write(new Text("Seasonal Average " + dataType + " for province \"" + province + "\" (" + seasonName + "):"), new Text(String.format("%.2f", seasonalAvg)));
        	}
        }
    }
    
    // Tính Seasonal Average cho Heat Index
    private void calculateSeasonalAverageForHeatIndex(TreeMap<String, Double> monthlyTempData, TreeMap<String, Double> monthlyHumidityData, String dataType, String province, Context context) throws IOException, InterruptedException {
        if (monthlyTempData == null || monthlyHumidityData == null || monthlyTempData.isEmpty() || monthlyHumidityData.isEmpty()) {
            context.write(new Text("Insufficient data for Seasonal Average of " + dataType + " for province \"" + province + "\""), new Text(""));
            return;
        }

        Map<Integer, Double> seasonalHeatIndexSum = new HashMap<>();
        Map<Integer, Integer> seasonalHeatIndexCount = new HashMap<>();

        for (Map.Entry<String, Double> tempEntry : monthlyTempData.entrySet()) {
            String yearMonth = tempEntry.getKey();
            if (monthlyHumidityData.containsKey(yearMonth)) {
                double temp = tempEntry.getValue();
                double humidity = monthlyHumidityData.get(yearMonth);
                double heatIndex = calculateHeatIndex(temp, humidity);

                int month = Integer.parseInt(yearMonth.substring(5, 7));
                int season = getSeasonFromMonth(month);

                seasonalHeatIndexSum.put(season, seasonalHeatIndexSum.getOrDefault(season, 0.0) + heatIndex);
                seasonalHeatIndexCount.put(season, seasonalHeatIndexCount.getOrDefault(season, 0) + 1);
            }
        }

        for (int season : seasonalHeatIndexSum.keySet()) {
            if (seasonalHeatIndexCount.get(season) > 0) {
                double seasonalAvg = seasonalHeatIndexSum.get(season) / seasonalHeatIndexCount.get(season);
                String seasonName = getSeasonName(season);
                context.write(new Text("Seasonal Average " + dataType + " for province \"" + province + "\" (" + seasonName + "):"), new Text(String.format("%.2f", seasonalAvg)));
            }
        }
    }
    
    private int getSeasonFromMonth(int month) {
        if (month >= 3 && month <= 5) {
            return 1; // Xuân
        } else if (month >= 6 && month <= 8) {
            return 2; // Hạ
        } else if (month >= 9 && month <= 11) {
            return 3; // Thu
        } else {
            return 4; // Đông
        }
    }

    private String getSeasonName(int season) {
        switch (season) {
            case 1:
                return "Spring";
            case 2:
                return "Summer";
            case 3:
                return "Autumn";
            case 4:
                return "Winter";
            default:
                return "Unknown";
        }
    }
    
}