import entity.NewRecordInfo;
import tool.ComUtil;

import java.io.*;

/**
 * load csv record to heap file
 * */
public class dbload {
    /**
     * per integer need byte length is 4
     * */
    private static int Byte_Len_For_Int = 4;
    /**
     * per integer need byte length is 4
     * */
    private static int Date_Time_LEN = 22;
    /**
     * per page length
     * */
    private static int perPageLen =0;
    /**
     * page bufffer
     * */
    private static byte[] pageBuf;
    /**
     * file name of storage
     * */
    private static String heapFileName;
    /**
     * csv file name
     * */
    private static String csvFileName;
    /**
     * count the number of records
     * */
    private static int dealRecordCnt =0;
    /**
     * Number of pages consumed
     * */
    private static int consumedPageCnt=0;
    /**
     * split char
     * */
    private final static String Split_Char=",";
    /**
     * record page buf position, initial value is 0
     * */
    private static int pageBufPos = 0;
    /**
     * deal  record from csv file
     * */
    public static void dealRecordFromCSV(){
        //firstly, read origin record from csv file
        try(BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(new FileInputStream(csvFileName)))) {
            //we need skip first row
            String row = bufferedReader.readLine();
            //read per row,deal per origin record
            while((row = bufferedReader.readLine())!=null){
                //next, we start to deal this origin record
                String[] colArr = row.split(Split_Char);
                //we only need ID,Date_Time,Sensor_ID,Sensor_Name,Hourly_Counts
                //acording to origin record, build new record info object
                NewRecordInfo newRecordInfo = new NewRecordInfo(colArr);
                //record object to byte array and into page buf
                putNewRecordIntoPageBuf(ComUtil.recordInfoToByteArr(newRecordInfo));
                //record the number of record from csv
                dealRecordCnt ++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static RandomAccessFile raf ;
    /**
     * put byte array of record into page buffer
     * */
    public static void putNewRecordIntoPageBuf(byte[] recordBR){
        //if can't put a record into page buffer, put pageBuf write to disk
        if(perPageLen < pageBufPos + recordBR.length  ){
            //if pageBufPos is 0, indicate pageBuf is empty.not need to write
            if(0 == pageBufPos ){
                return;
            }
            // use RandomAccessFile class to write file
            try {
                //move the point of file to blank
                raf.seek(consumedPageCnt * perPageLen);
                raf.write(pageBuf);
            } catch (IOException e) {
                e.printStackTrace();
            }
            // update global info
            consumedPageCnt++;
            //clear page buffer
            pageBuf = new byte[perPageLen];
            pageBufPos = 0;
        }
        System.arraycopy(recordBR,0,pageBuf,pageBufPos,recordBR.length);
        pageBufPos += recordBR.length;
    }
    public static void main(String[] args) throws FileNotFoundException {
        if(args.length != 3){
            //arugments is too letter,must be 3 (-p pagesize datafile)
            return;
        }
        //get per page's length
        perPageLen = Integer.valueOf(args[1]);
        //initial page buffer
        pageBuf = new byte[perPageLen];
        //build file name of storage
        heapFileName = "heap." + perPageLen;
        //get csv file name
        csvFileName = args[2];
        //while new running ,delete old file
        File lastFile = new File(heapFileName);
        lastFile.delete();
        raf =new RandomAccessFile(heapFileName, "rw");
        // for statistical time
        long time1 =0;
        long time2 = 0;
        time1 = System.currentTimeMillis();
        dealRecordFromCSV();
        time2 = System.currentTimeMillis();
        System.out.println("\rhadload "+dealRecordCnt+" records");
        System.out.println("comsumed "+ consumedPageCnt +" pages");
        System.out.println("finish in "+ (time2 - time1) + " ms");
    }

}
