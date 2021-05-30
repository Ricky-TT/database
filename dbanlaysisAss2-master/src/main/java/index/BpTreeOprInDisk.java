package index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

/**
 * help read and save b+ tree
 * */
public class BpTreeOprInDisk {
    /**
     * write the tree
     * */
    public static boolean writeBpTree(int pageSize,BpTree bpTree){
        BpNode root = bpTree.root;
        int nextPageNum =0;
        //set per node's storage page number
        Queue<BpNode> bpNodeQueue = new LinkedList<>();
        bpNodeQueue.offer(root);
        root.pageNum = nextPageNum;
        nextPageNum += 1;
        while(!bpNodeQueue.isEmpty()){
            int size = bpNodeQueue.size();
            for (int i = 0; i < size; i++) {
                BpNode node = bpNodeQueue.poll();
                if(node != null &&!node.leafFlag) {
                    for(BpNode childNode:node.childeNodeList) {
                        bpNodeQueue.add(childNode);
                        childNode.pageNum = nextPageNum;
                        nextPageNum++;
                    }
                }
            }
        }
        try(RandomAccessFile raf = new RandomAccessFile("tree."+pageSize, "rw")) {
            //level scan
            bpNodeQueue = new LinkedList<>();
            bpNodeQueue.offer(root);
            while(!bpNodeQueue.isEmpty()){
                int size = bpNodeQueue.size();
                for (int i = 0; i < size; i++) {
                    BpNode node = bpNodeQueue.poll();
                    short kSize = (short) node.kvInNodeList.size();
                    //save node content to byte array
                    int nodebyteSize = HeapFileConfig.ByteSizeOfShort+HeapFileConfig.ByteSizeOfChar+
                            kSize*HeapFileConfig.ByteSizeOfKey+HeapFileConfig.ByteSizeOfInt*(kSize+1);
                    byte[] nodeBr = new byte[nodebyteSize];
                    //reset position in nodeBr
                    int nextOffset =0;
                    //save key count
                    byte[] kSizeBr = ByteBuffer.allocate(HeapFileConfig.ByteSizeOfShort).putShort(kSize).array();
                    System.arraycopy(kSizeBr,0,nodeBr,nextOffset, kSizeBr.length);
                    nextOffset+= HeapFileConfig.ByteSizeOfShort;
                    //save leaf flag
                    boolean leafFlag = node.leafFlag;
                    byte[] isLeafBr = ByteBuffer.allocate(HeapFileConfig.ByteSizeOfChar).putChar((char) (leafFlag ? 1 : 0)).array();
                    System.arraycopy(isLeafBr,0,nodeBr,nextOffset, isLeafBr.length);
                    nextOffset+= HeapFileConfig.ByteSizeOfChar;
                    //save k-v pair, value|key|value|key|value....
                    int keyIdx = 0;
                    for (KvInNode key : node.kvInNodeList) {
                        //save value
                        if(!node.leafFlag){
                            bpNodeQueue.offer(node.childeNodeList.get(keyIdx));
                            byte[] pageIdxBr = ByteBuffer.allocate(HeapFileConfig.ByteSizeOfInt)
                                    .putInt(node.childeNodeList.get(keyIdx).pageNum).array();
                            System.arraycopy(pageIdxBr,0,nodeBr,nextOffset, pageIdxBr.length);
                            nextOffset+= HeapFileConfig.ByteSizeOfInt;
                        }else{
                            byte[] dataPtr = ByteBuffer.allocate(HeapFileConfig.ByteSizeOfInt)
                                    .putInt(node.kvInNodeList.get(keyIdx).recordAddr).array();
                            System.arraycopy(dataPtr,0,nodeBr,nextOffset, dataPtr.length);
                            nextOffset+= HeapFileConfig.ByteSizeOfInt;
                        }
                        //save key
                        byte[] keyBr = ByteBuffer.allocate(HeapFileConfig.ByteSizeOfKey).put(key.key.getBytes()).array();
                        System.arraycopy(keyBr,0,nodeBr,nextOffset, keyBr.length);
                        nextOffset+= HeapFileConfig.ByteSizeOfKey;
                        keyIdx++;
                    }
                    //if not leaf node ,need to save last value
                    if(!node.leafFlag) {
                        bpNodeQueue.offer(node.childeNodeList.get(keyIdx));
                        byte[] pageIdxBr = ByteBuffer.allocate(HeapFileConfig.ByteSizeOfInt)
                                .putInt(node.childeNodeList.get(keyIdx).pageNum).array();
                        System.arraycopy(pageIdxBr,0,nodeBr,nextOffset, pageIdxBr.length);
                        nextOffset+= HeapFileConfig.ByteSizeOfInt;
                    }
                    //write file
                    raf.seek(node.pageNum *pageSize);
                    raf.write(nodeBr);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
    /**
     * read tree
     * */
    public static boolean readBpTree(int pageSize, BpTree bpTree){
        //firstly, read root node
        BpNode newRootNode = new BpNode(false,true);
        byte[] kSizeBr, leafFlagBr,valueBr,keyBr;
        short kSize;
        try( RandomAccessFile  raf = new RandomAccessFile("tree."+pageSize, "rw")) {
            //for a page content
            byte[] pageContent = new byte[pageSize];
            //offset in page
            int offsetP =0;
            raf.seek(offsetP);
            raf.read(pageContent);
            //read kSize
            kSizeBr  = new byte[HeapFileConfig.ByteSizeOfShort];
            System.arraycopy(pageContent, offsetP, kSizeBr, 0, HeapFileConfig.ByteSizeOfShort);
            offsetP+=HeapFileConfig.ByteSizeOfShort;
            kSize = ByteBuffer.wrap(kSizeBr).getShort();
            //read leaf flag
            byte[] rootIsLeafBr = new byte[HeapFileConfig.ByteSizeOfChar];
            System.arraycopy(pageContent, offsetP, rootIsLeafBr, 0, HeapFileConfig.ByteSizeOfChar);
            offsetP+=HeapFileConfig.ByteSizeOfChar;
            newRootNode.leafFlag = ByteBuffer.wrap(rootIsLeafBr).getChar() != 0;

            //storage child node's page idx
            newRootNode.childrenPageIndexs = new ArrayList<>();
            for (int i = 0; i < kSize; i++) {
                if(!newRootNode.leafFlag) {
                    //read page number of child node
                    valueBr = new byte[HeapFileConfig.ByteSizeOfInt];
                    System.arraycopy(pageContent, offsetP, valueBr, 0, HeapFileConfig.ByteSizeOfInt);
                    offsetP += HeapFileConfig.ByteSizeOfInt;
                    newRootNode.childrenPageIndexs.add(ByteBuffer.wrap(valueBr).getInt());
                }else{
                    //read record address
                    valueBr = new byte[HeapFileConfig.ByteSizeOfInt];
                    System.arraycopy(pageContent, offsetP, valueBr, 0, HeapFileConfig.ByteSizeOfInt);
                    offsetP += HeapFileConfig.ByteSizeOfInt;
                    newRootNode.kvInNodeList.get(i).recordAddr = ByteBuffer.wrap(valueBr).getInt();
                }
                keyBr = new byte[HeapFileConfig.ByteSizeOfKey];
                System.arraycopy(pageContent, offsetP, keyBr, 0, HeapFileConfig.ByteSizeOfKey);
                offsetP+=HeapFileConfig.ByteSizeOfKey;
                KvInNode values = new KvInNode(new String(keyBr).trim());
                newRootNode.kvInNodeList.add(values);
            }

            if(!newRootNode.leafFlag) {
                byte[] pageIdxBr = new byte[HeapFileConfig.ByteSizeOfInt];
                System.arraycopy(pageContent, offsetP, pageIdxBr, 0, HeapFileConfig.ByteSizeOfInt);
                offsetP += HeapFileConfig.ByteSizeOfInt;
                newRootNode.childrenPageIndexs.add(ByteBuffer.wrap(pageIdxBr).getInt());
            }
            //set root info
            bpTree.root = newRootNode;
            bpTree.leafHeadNode = null;
            //if only one node ,return
            if(bpTree.root.leafFlag){
                bpTree.leafHeadNode = bpTree.root;
                return true;
            }
            Queue<BpNode> bpNodeQueue = new LinkedList<>();
            bpNodeQueue.offer(newRootNode);
            while(!bpNodeQueue.isEmpty()){
                int size = bpNodeQueue.size();
                //read children node
                for (int i = 0; i < size; i++) {
                    BpNode parentNode = bpNodeQueue.poll();
                    //set leaf node list
                    if(parentNode.leafFlag){
                        bpTree.insertLeafList(parentNode);
                        continue;
                    }
                    //according to page number, reading child node info
                    for (int j = 0; j < parentNode.childrenPageIndexs.size(); j++) {
                        try {
                            raf.seek(parentNode.childrenPageIndexs.get(j) * pageSize);
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                        pageContent = new byte[pageSize];
                        raf.read(pageContent);
                        //reset offset
                        offsetP = 0;
                        //read kSize
                        kSizeBr = new byte[HeapFileConfig.ByteSizeOfShort];
                        System.arraycopy(pageContent, offsetP, kSizeBr, 0, HeapFileConfig.ByteSizeOfShort);
                        offsetP+=HeapFileConfig.ByteSizeOfShort;
                        kSize = ByteBuffer.wrap(kSizeBr).getShort();
                        //read leaf flag
                        leafFlagBr = new byte[HeapFileConfig.ByteSizeOfShort];
                        System.arraycopy(pageContent, offsetP, leafFlagBr, 0, HeapFileConfig.ByteSizeOfShort);
                        offsetP+=HeapFileConfig.ByteSizeOfShort;
                        boolean isLeaf = ByteBuffer.wrap(leafFlagBr).getChar() != 0;
                        BpNode newNode = new BpNode(isLeaf,false);

                        newNode.childrenPageIndexs = new ArrayList<>();
                        for (int k = 0; k < kSize; k++) {
                            valueBr = new byte[HeapFileConfig.ByteSizeOfInt];
                            System.arraycopy(pageContent, offsetP, valueBr, 0, HeapFileConfig.ByteSizeOfInt);
                            offsetP += HeapFileConfig.ByteSizeOfInt;
                            //read key
                            keyBr = new byte[HeapFileConfig.ByteSizeOfKey];
                            try {
                                System.arraycopy(pageContent, offsetP, keyBr, 0, HeapFileConfig.ByteSizeOfKey);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            offsetP+=HeapFileConfig.ByteSizeOfKey;
                            KvInNode values = new KvInNode(new String(keyBr).trim());
                            newNode.kvInNodeList.add(values);
                            //analyize value
                            if(!newNode.leafFlag) {
                                //page number
                                newNode.childrenPageIndexs.add(ByteBuffer.wrap(valueBr).getInt());
                            }else{
                                //record address
                                newNode.kvInNodeList.get(k).recordAddr = ByteBuffer.wrap(valueBr).getInt();
                            }
                        }
                        if(!newNode.leafFlag) {
                            // read page num of child node
                            valueBr = new byte[HeapFileConfig.ByteSizeOfInt];
                            System.arraycopy(pageContent, offsetP, valueBr, 0, HeapFileConfig.ByteSizeOfInt);
                            offsetP += HeapFileConfig.ByteSizeOfInt;
                            newNode.childrenPageIndexs.add(ByteBuffer.wrap(valueBr).getInt());
                        }
                        parentNode.childeNodeList.add(newNode);
                        newNode.parent = parentNode;
                        bpNodeQueue.add(newNode);
                    }
                    parentNode.childrenPageIndexs = null;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}
