package index;

import entity.NewRecordInfo;
import tool.ComUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * index
 * */
public class BpTreeIndex {
    /**
     * generate k-v pair
     * */
    private static KvInNode generateKeyValue(String key, int value) {
        KvInNode kvInNode = new KvInNode(key);
        kvInNode.recordAddr = value;
        return kvInNode;
    }
    public static KvInNode generateKeyValue(String key) {
        return new KvInNode(key);
    }
    /**
     * for each record and insert key
     * */
    public static void constructBpIndex(int pageSize, BpTree bpTree){
        //judge heap file if exist
        File searchFile = new File("heap."+pageSize);
        dealFileContext(pageSize,bpTree);
        //write tree
        BpTreeOprInDisk.writeBpTree(pageSize,bpTree);
    }
    /**
     * deal heap file ,to search text
     * */
    private static void dealFileContext(int pageSize, BpTree bpTree) {
        try(BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream("heap."+pageSize))) {
            byte[] pageBuf = new byte[pageSize];
            //read a page from heap file to page buffer
            int len = 0;
            int pageIdx = 0;
            while((len=inputStream.read(pageBuf))!=-1){
                if(pageSize != len){
                    System.err.println("no a page");
                }
                //next,search text in a page
                searchTextInPage(pageBuf,pageSize, pageIdx,bpTree);
                pageIdx += 1;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static int Min_Len_Record = 38;
    private static int Date_Time_Len = 22;
    private static void searchTextInPage(byte[] pageBuf,int pageSize,int pageIdx,BpTree bpTree) {
        //position in page
        int pagePos = 0;
        //for getting integer value
        byte[] tmpBRForInt = new byte[HeapFileConfig.ByteSizeOfInt];
        //for getting date time
        byte[] tmpDateTimeBR = new byte[Date_Time_Len];
        //judge if have next record
        while (pagePos+Min_Len_Record < pageSize) {
            //for record the offset
            int originPagePos = pagePos;
            //read ID,Sensor_ID, hourly_count,date_time sensor_name_len
            System.arraycopy(pageBuf, pagePos, tmpBRForInt, 0, HeapFileConfig.ByteSizeOfInt);
            int id = ByteBuffer.wrap(tmpBRForInt).getInt();pagePos+=HeapFileConfig.ByteSizeOfInt;
            if(id == 0){
                break;
            }
            System.arraycopy(pageBuf, pagePos, tmpBRForInt, 0, HeapFileConfig.ByteSizeOfInt);
            int sensorId = ByteBuffer.wrap(tmpBRForInt).getInt();pagePos+=HeapFileConfig.ByteSizeOfInt;
            System.arraycopy(pageBuf, pagePos, tmpBRForInt, 0, HeapFileConfig.ByteSizeOfInt);
            int hourly = ByteBuffer.wrap(tmpBRForInt).getInt();pagePos+=HeapFileConfig.ByteSizeOfInt;
            System.arraycopy(pageBuf, pagePos, tmpDateTimeBR, 0, Date_Time_Len);
            String dateTime =new String(tmpDateTimeBR);pagePos+=Date_Time_Len;
            System.arraycopy(pageBuf, pagePos, tmpBRForInt, 0, HeapFileConfig.ByteSizeOfInt);
            int sensorNameLen = ByteBuffer.wrap(tmpBRForInt).getInt();
            byte[] sensorNameBR = new byte[sensorNameLen];pagePos+=HeapFileConfig.ByteSizeOfInt;
            System.arraycopy(pageBuf, pagePos, sensorNameBR, 0, sensorNameLen);
            String sensorName = new String(sensorNameBR); pagePos+=sensorNameLen;
            if(sensorNameLen == 0){
                System.err.println("sensorName is null");
            }
            NewRecordInfo newRecordInfo = new NewRecordInfo(id,sensorId,hourly,dateTime,sensorNameLen,sensorName);
            //get index filed,insert tree
            String combinStr = newRecordInfo.getSensor_ID()+"-"+newRecordInfo.getDate_Time();
            KvInNode kvInNode = generateKeyValue(combinStr, pageIdx*pageSize+originPagePos);
            bpTree.insertKey(kvInNode);
        }
    }
    /**
     * read record by address
     * */
    public static String readRecordByAddress(int address,int pageSize){
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile("heap."+pageSize, "rw");
            raf.seek(address);
            byte[] tmpBRForInt = new byte[HeapFileConfig.ByteSizeOfInt];
            byte[] tmpDateTimeBR = new byte[Date_Time_Len];
            //read ID,Sensor_ID, hourly_count,date_time sensor_name_len
            raf.read(tmpBRForInt);
            int id = ByteBuffer.wrap(tmpBRForInt).getInt();
            raf.read(tmpBRForInt);
            int sensorId = ByteBuffer.wrap(tmpBRForInt).getInt();
            raf.read(tmpBRForInt);
            int hourly = ByteBuffer.wrap(tmpBRForInt).getInt();
            raf.read(tmpDateTimeBR);
            String dateTime =new String(tmpDateTimeBR);
            raf.read(tmpBRForInt);
            int sensorNameLen = ByteBuffer.wrap(tmpBRForInt).getInt();
            byte[] sensorNameBR = new byte[sensorNameLen];
            raf.read(sensorNameBR);
            String sensorName = new String(sensorNameBR);
            if(sensorNameLen == 0){
                System.err.println("sensorName is null");
            }
            NewRecordInfo newRecordInfo = new NewRecordInfo(id,sensorId,hourly,dateTime,sensorNameLen,sensorName);
            return ComUtil.recoverRecord(newRecordInfo);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if(raf !=null){
                try {
                    raf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * select record
     * */
    public static List<String> selectRecordByIndex(String conditionText, int pageSize){
        BpTree tree = new BpTree();
        //this filed will set in readBpTree
        tree.root =null;
        tree.leafHeadNode = null;
        long bt= System.currentTimeMillis();
        BpTreeOprInDisk.readBpTree(pageSize,tree);
        long et = System.currentTimeMillis();
        System.out.println("read tree = "+(et-bt));
        KvInNode searchKvInNode = BpTreeIndex.generateKeyValue(conditionText);
        if(conditionText.length()< HeapFileConfig.LengthOfKey){
            searchKvInNode.rangeFlag = true;
        }
        if(conditionText.length() == HeapFileConfig.LengthOfKey){
            searchKvInNode.rangeFlag =conditionText.split("-")[1].length()< 22;
        }
        KvInNode kvInNode = tree.selectKeyInTree(searchKvInNode);
        if(kvInNode== null) return null;
        List<String> results =new ArrayList<>();
        if(!searchKvInNode.rangeFlag) {
            results.add(readRecordByAddress(kvInNode.recordAddr, pageSize));
        }else{
            for (Integer pos: kvInNode.selectRsByRangeList){
                results.add(readRecordByAddress(pos, pageSize));
            }
        }
        return results;
    }
}
