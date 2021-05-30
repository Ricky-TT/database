import entity.NewRecordInfo;
import tool.ComUtil;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * according to text ,find record
 * */
public class dbquery {
    /**
     * per integer need byte length is 4
     * */
    private static int Byte_Len_For_Int = 4;
    /**
     * select condition string
     * */
    private static String selectText;
    /**
     * per page length
     * */
    private static int perPageLen;
    /**
     * heap file name
     * */
    private static String heapFileName;
    /**
     * match count
     * */
    private static int matchCnt = 0;
    public static void main(String[] args) {
        //analysis text from arugments
        selectText = "";
        for (int i = 0; i < args.length - 1; i++) {
            selectText += args[i] + " ";
        }
        selectText = selectText.substring(0,selectText.length()-1);
        //analysis per page length
        perPageLen = Integer.parseInt(args[args.length - 1]);
        //get heap file name
        heapFileName = "heap." + perPageLen;
        //judge heap file if exist
        File searchFile = new File(heapFileName);
        if (!searchFile.exists() || !searchFile.isFile()) {
            System.err.println(heapFileName + "is not exist");
            return;
        }
        //begin time
        long time1 = System.currentTimeMillis();
        dealFileContext();
        //end time
        long time2 = System.currentTimeMillis();
        System.out.println("match count="+matchCnt);
        System.out.println("finish in " + (time2 - time1) + " ms");
    }
    /**
     * deal heap file ,to search text
     * */
    private static void dealFileContext() {
        try(BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(heapFileName))) {
            byte[] pageBuf = new byte[perPageLen];
            //read a page from heap file to page buffer
            int len = 0;
            int pageCnt = 0;
            while((len=inputStream.read(pageBuf))!=-1){
                if(perPageLen != len){
                    System.err.println("no a page");
                }
                //next,search text in a page
                searchTextInPage(pageBuf);
                pageCnt ++;
            }
            System.out.println(pageCnt);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * the min length of record
     * */
    private static int Min_Len_Record = 38;
    private static int Date_Time_Len = 22;
    /**
     * search text in a page
     * */
    private static void searchTextInPage(byte[] pageBuf) {
        //position in page
        int pagePos = 0;
        //for getting integer value
        byte[] tmpBRForInt = new byte[Byte_Len_For_Int];
        //for getting date time
        byte[] tmpDateTimeBR = new byte[Date_Time_Len];
        //judge if have next record
        while (pagePos+Min_Len_Record < perPageLen) {
            //read ID,Sensor_ID, hourly_count,date_time sensor_name_len
            System.arraycopy(pageBuf, pagePos, tmpBRForInt, 0, Byte_Len_For_Int);
            int id = ByteBuffer.wrap(tmpBRForInt).getInt();pagePos+=Byte_Len_For_Int;
            if(id == 0){
                break;
            }
            System.arraycopy(pageBuf, pagePos, tmpBRForInt, 0, Byte_Len_For_Int);
            int sensorId = ByteBuffer.wrap(tmpBRForInt).getInt();pagePos+=Byte_Len_For_Int;
            System.arraycopy(pageBuf, pagePos, tmpBRForInt, 0, Byte_Len_For_Int);
            int hourly = ByteBuffer.wrap(tmpBRForInt).getInt();pagePos+=Byte_Len_For_Int;
            System.arraycopy(pageBuf, pagePos, tmpDateTimeBR, 0, Date_Time_Len);
            String dateTime =new String(tmpDateTimeBR);pagePos+=Date_Time_Len;
            System.arraycopy(pageBuf, pagePos, tmpBRForInt, 0, Byte_Len_For_Int);
            int sensorNameLen = ByteBuffer.wrap(tmpBRForInt).getInt();
            byte[] sensorNameBR = new byte[sensorNameLen];pagePos+=Byte_Len_For_Int;
            System.arraycopy(pageBuf, pagePos, sensorNameBR, 0, sensorNameLen);
            String sensorName = new String(sensorNameBR); pagePos+=sensorNameLen;
            if(sensorNameLen == 0){
                System.err.println("sensorName is null");
            }
            NewRecordInfo newRecordInfo = new NewRecordInfo(id,sensorId,hourly,dateTime,sensorNameLen,sensorName);
            //match SDT_Name
            if (ComUtil.compareForQuery(selectText,newRecordInfo)) {
                //print info
                matchCnt ++;
                System.out.println(ComUtil.recoverRecord(newRecordInfo));
            }
        }
    }
}
