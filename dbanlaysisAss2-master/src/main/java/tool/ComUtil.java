package tool;

import entity.NewRecordInfo;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * provide some common tool
 * */
public class ComUtil {
    /**
     * per integer need byte length is 4
     * */
    private static int Byte_Len_For_Int = 4;
    /**
     * record object to byte[]
     * */
    public static byte[] recordInfoToByteArr(NewRecordInfo record){
        List<byte[]> byteArrList = new ArrayList<>();
        //The number of bytes occupied by the record
        int recordLength =0;
        byte[] idBR = ByteBuffer.allocate(Byte_Len_For_Int).putInt(record.id).array();
        recordLength += idBR.length;byteArrList.add(idBR);
        byte[] sensorIDBR = ByteBuffer.allocate(Byte_Len_For_Int).putInt(record.getSensor_ID()).array();
        recordLength += sensorIDBR.length;byteArrList.add(sensorIDBR);
        byte[] hourlyCountBR =ByteBuffer.allocate(Byte_Len_For_Int).putInt(record.getHourly_Counts()).array();
        recordLength += hourlyCountBR.length;byteArrList.add(hourlyCountBR);
        byte[] dateTimeBR = record.getDate_Time().getBytes();
        recordLength += dateTimeBR.length;byteArrList.add(dateTimeBR);
        if(record.sensor_Name_len == 0){
            System.out.println("");
        }
        byte[] sensorNameLenBR = ByteBuffer.allocate(Byte_Len_For_Int).putInt(record.sensor_Name_len).array();
        recordLength += sensorNameLenBR.length;byteArrList.add(sensorNameLenBR);
        byte[] sensorNameBR= record.getSensor_Name().getBytes();
        recordLength += sensorNameBR.length;byteArrList.add(sensorNameBR);
        //use record buffer to save above byte arrays
        byte[] buffer = new byte[recordLength];
        //next, copy above byte arrays to  buffer
        //record next pos in buffer
        int nextPosition =0;
        for (byte[] tmpBR: byteArrList){
            System.arraycopy(tmpBR,0,buffer,nextPosition, tmpBR.length);
            //update nextPosition
            nextPosition += tmpBR.length;
        }
        return buffer;
    }
    /**
     * compare text and newRecordInfo info
     * */
    public static boolean compareForQuery(String text,NewRecordInfo newRecordInfo){
        String combinStr = newRecordInfo.getSensor_ID()+"-"+newRecordInfo.getDate_Time();
        return combinStr.contains(text);
    }
    /**
     * recover newRecordInfo to origin recordInfo
     * */
    public static String recoverRecord(NewRecordInfo newRecordInfo){
        StringBuilder sb = new StringBuilder();
        sb.append(newRecordInfo.getId()).append(',');
        sb.append(newRecordInfo.Date_Time).append(',');
        int[] otherFiled = resolveDay(newRecordInfo.Date_Time);
        sb.append(otherFiled[0]).append(',');//year
        sb.append(monthStrs[otherFiled[1]]).append(',');//month
        sb.append(otherFiled[2]).append(',');//mday
        sb.append(weekDayStrs[otherFiled[3]]).append(',');//day
        sb.append(otherFiled[4]).append(',');//time
        sb.append(newRecordInfo.getSensor_ID()).append(',');
        sb.append(newRecordInfo.getSensor_Name()).append(',');
        sb.append(newRecordInfo.getHourly_Counts());
        return sb.toString();
    }
    /**
     * resolve year month mday day time from date_time
     * example 11/01/2019 05:00:00 PM
     * */
    private static int[] resolveDay(String dateTime){
        int[] res = new int[5];
        String[] str1 = dateTime.split(" ");
        boolean isPM = str1[2].equals("PM");
        //resolve year month mday
        String[] ymd = str1[0].split("/");
        res[0] = Integer.parseInt(ymd[2]);//year
        res[1] = Integer.parseInt(ymd[0]);//month
        res[2] = Integer.parseInt(ymd[1]);//mday
        //cal week day
        res[3] = dateToWeek(str1[0]);
        String[] timeStrs = str1[1].split(":");
        int basehour = Integer.parseInt(timeStrs[0]);
        if(!isPM){
            if(basehour !=12){
                res[4] = basehour;
            }else{
                res[4] = 0;
            }
        }else{
            if(basehour !=12){
                res[4] = basehour+12;
            }else{
                res[4] = 12;
            }
        }
        return res;
    }
    private static String[] weekDayStrs =  { "","Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday" };
    private static String[] monthStrs = {"","January","February","March","April","May","June","July","August","September","October","November","December"};
    /**
     * get week day by date
     * */
    public static int dateToWeek(String datetime) {
        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy");
        Calendar c = Calendar.getInstance();
        try {
            c.setTime(format.parse(datetime));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        int dayForWeek = 0;
        if (c.get(Calendar.DAY_OF_WEEK) == 1) {
            dayForWeek = 7;
        } else {
            dayForWeek = c.get(Calendar.DAY_OF_WEEK) - 1;
        }
        return dayForWeek;
    }

    /**
     * caches week mapper
     */
    private static Map<String, Integer> cachedWeekMapper = null;

    /**
     * a mapper that converts month from string to integer
     *
     * @return
     */
    public static int getWeekNum(String weekStr) {
        if (cachedWeekMapper != null) return cachedWeekMapper.get(weekStr.toUpperCase());
        Map<String, Integer> monthMap = new HashMap<String, Integer>() {{
            put("JANUARY", 1);
            put("FEBRUARY", 2);
            put("MARCH", 3);
            put("APRIL", 4);
            put("MAY", 5);
            put("JUNE", 6);
            put("JULY", 7);
            put("AUGUST", 8);
            put("SEPTEMBER", 9);
            put("OCTOBER", 10);
            put("NOVEMBER", 11);
            put("DECEMBER", 12);
        }};
        cachedWeekMapper = monthMap;
        return monthMap.get(weekStr.toUpperCase());
    }

    public static void main(String[] args) {
        System.out.println(dateToWeek("11/01/2019"));
    }

}
