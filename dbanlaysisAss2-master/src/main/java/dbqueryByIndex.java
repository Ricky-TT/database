import index.BpTreeIndex;

import java.util.List;

public class dbqueryByIndex {
    /**
     * SDT_NAME
     */
    public static String SDTNAME;
    /**
     * binary fileName
     */
    public static String fileName;
    /**
     * size of a page
     */
    public static int pageSize = 0;
    /**
     * match count
     */
    public static int matchCnt = 0;

    public static void main(String[] args) {
        long beginTime = 0;
        long endTime = 0;

        StringBuilder sb = new StringBuilder();
        int pageSize = Integer.parseInt(args[args.length -1]);
        for (int i = 0; i < args.length - 1; i++) {
            sb.append(args[i]).append(" ");
        }
        sb.deleteCharAt(sb.length() - 1);
        //get search key
        SDTNAME = sb.toString();
        beginTime = System.currentTimeMillis();
        //find record
        List<String> recordByIndex = BpTreeIndex.selectRecordByIndex(SDTNAME, pageSize);
        int recordResultCnt = 0;
        if(recordByIndex!=null) {
            recordResultCnt = recordByIndex.size();
            for (String recordStr : recordByIndex) {
                System.out.println(recordStr);
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println(endTime - beginTime+"ms");
        System.out.println("find "+recordResultCnt+" records");
    }


}
