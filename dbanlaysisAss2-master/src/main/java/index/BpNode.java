package index;

import java.util.ArrayList;
import java.util.List;
/**
 * Node in b+ tree
 * */
public class BpNode {
    /**is leaf*/
    public boolean leafFlag = true;
    /**
     * is root
     */
    public boolean rootFlag;
    /**key list*/
    public List<KvInNode> kvInNodeList = new ArrayList<>();
    public List<BpNode> childeNodeList;

    public BpNode parent;
    public BpNode prev;
    public BpNode next;

    /** for storage*/
    public List<Integer> childrenPageIndexs;
    public int pageNum;
    public BpNode(){

    }
    public BpNode(boolean leafFlag) {
        if (!leafFlag) {
            this.leafFlag = false;
            childeNodeList = new ArrayList<>();
        }
    }

    public BpNode(boolean leafFlag, boolean rootFlag) {
        this.rootFlag = rootFlag;
        if (!leafFlag) {
            this.leafFlag = false;
            childeNodeList = new ArrayList<>();
        }

    }
    /**
     * Find the location of the key
     * */
    public KvInNode select(KvInNode key) {
        if (leafFlag) {
            if(key.rangeFlag == false) {
                KvInNode resultNode = null;
                for(int i =0;i< kvInNodeList.size();i++) {
                    if (0 == key.wholeMatch(kvInNodeList.get(i))) {
                        resultNode = kvInNodeList.get(i);
                    }
                }
                return resultNode;
            }
            boolean isExist = false;
            key.selectRsByRangeList = new ArrayList<>();
            //firstly find first Node
            for (int i =0;i< kvInNodeList.size();i++) {
                if (kvInNodeList.get(i).leafMathch(key)) {
                    key.selectRsByRangeList.add(kvInNodeList.get(i).recordAddr);
                }
            }
            if(key.selectRsByRangeList.isEmpty()){
                return null;
            }
            BpNode curNode = this.next;
            BpNode curNode0 = this.prev;
            //right find
            w1:
            while(curNode!=null){
                for (KvInNode kvInNode : curNode.kvInNodeList) {
                    if (kvInNode.leafMathch(key)) {
                        key.selectRsByRangeList.add(kvInNode.recordAddr);
                    }else{
                        break w1;
                    }
                }
                curNode = curNode.next;
            }
            w2:
            while( null != curNode0){
                for (int i = curNode0.kvInNodeList.size()-1; i >=0 ; i--) {
                    if (curNode0.kvInNodeList.get(i).leafMathch(key)) {
                        key.selectRsByRangeList.add(curNode0.kvInNodeList.get(i).recordAddr);
                    }else{
                        break w2;
                    }
                }
                curNode0 = curNode0.prev;
            }
            return key;
        } else {
            int firstIdx = 0, lastIdx = kvInNodeList.size() -1;
            if (key.wholeMatch(kvInNodeList.get(firstIdx)) < 0) {
                return childeNodeList.get(firstIdx).select(key);
            }
            if (key.wholeMatch(kvInNodeList.get(lastIdx)) >= 0) {
                return childeNodeList.get(lastIdx).select(key);
            }
            for (int i = 0; i < (kvInNodeList.size() - 1); i++) {
                if (key.wholeMatch(kvInNodeList.get(i)) >= 0 && key.wholeMatch(kvInNodeList.get(i + 1)) < 0) {
                    return childeNodeList.get(i + 1).select(key);
                }
            }
            return null;
        }

    }
    /**Insert new kv pair*/
    public void insertKv(BpTree tree, KvInNode key) {
        if (this.leafFlag) {
            insertKvInLeaf(key,tree);
        }
        else {
            insertKvNotInLeaf(key,tree);
        }
    }
    /**
     * if this node is leaf
     * */
    public void insertKvInLeaf(KvInNode key, BpTree tree){
        if (!(kvInNodeList.size() >= (HeapFileConfig.MaxKvCnt - 1))) {
            insert(key);
        } else {
            //Need to be split into left and right nodes
            BpNode left = new BpNode(true);
            BpNode right = new BpNode(true);
            if (null != prev ) {
                prev.next = left;
                left.prev = prev;
            } else {
                tree.leafHeadNode = left;
            }
            if (null != next  ) {
                next.prev = right;

                right.next = next;
            }
            right.prev = left;
            left.next = right;

            insert(key);
            int leftSize = upper(kvInNodeList.size());
            // Left and right node copy
            for (int i = 0; i < leftSize; i++) {
                left.kvInNodeList.add(kvInNodeList.get(i));
            }
            int rightSize = kvInNodeList.size() - leftSize;
            for (int i = 0; i < rightSize; i++) {
                right.kvInNodeList.add(kvInNodeList.get(leftSize + i));
            }
            // Not the root node
            if (!rootFlag) {
                // Adjust the relationship between parent and child nodes
                // Find the position of the current node in the parent node
                int index = parent.childeNodeList.indexOf(this);

                left.parent = parent;
                right.parent = parent;
                // Delete current pointer
                parent.childeNodeList.remove(this);
                // Add the pointer of the split node to the parent node
                parent.childeNodeList.add(index, left);
                parent.childeNodeList.add(index + 1, right);

                parent.insert(right.kvInNodeList.get(0));
                parent.changeNode(tree);

            } else {
                // This node is the root node
                // Regenerate the root node
                rootFlag = false;
                BpNode rootNode = new BpNode(false, true);
                tree.root = rootNode;
                rootNode.childeNodeList.add(left);
                rootNode.childeNodeList.add(right);
                left.parent = rootNode;
                right.parent = rootNode;

                // Insert Keyword at Root Node
                rootNode.insert(right.kvInNodeList.get(0));
            }
        }
    }
    /**
     * if this node is not leaf
     * */
    public void insertKvNotInLeaf(KvInNode key, BpTree tree){
        // If it is not a leaf node, search down the pointer
        int firstIdx = 0, lastIdx = childeNodeList.size() - 1;
        if (key.wholeMatch(kvInNodeList.get(firstIdx)) < 0) {
            childeNodeList.get(firstIdx).insertKv( tree,key);
        } else if (key.wholeMatch(kvInNodeList.get(kvInNodeList.size()-1)) >= 0) {
            childeNodeList.get(lastIdx).insertKv(tree,key);
        } else {
            // Traversal comparison
            int i =0;
            while(i< kvInNodeList.size() - 1) {
                if (key.wholeMatch(kvInNodeList.get(i)) >= 0) {
                    if(key.wholeMatch(kvInNodeList.get(i + 1)) < 0){
                        childeNodeList.get(i + 1).insertKv(tree, key);
                        break;
                    }
                }
                i += 1;
            }
        }
    }
    /**
     * After inserting the keyword in the non-leaf node, check whether it needs to be split
     */
    private void changeNode(BpTree tree) {
        // Need to split
        if (isNodeToSplit()) {
            BpNode left = new BpNode(false);
            BpNode right = new BpNode(false);
            int pLeftSize = upper(childeNodeList.size());
            int pRightSize = childeNodeList.size() - pLeftSize;   //fix bug
            // Keyword promoted to parent node
            KvInNode keyToParent = kvInNodeList.get(pLeftSize - 1);
            // Copy the keyword on the left
            for (int i = 0; i < (pLeftSize - 1); i++) {
                left.kvInNodeList.add(kvInNodeList.get(i));
            }
            // Copy the pointer on the left
            for (int i = 0; i < pLeftSize; i++) {
                left.childeNodeList.add(childeNodeList.get(i));
                left.childeNodeList.get(i).parent = left;
            }
            // Copy the keyword on the right, and the first keyword on the right is promoted to the parent node
            for (int i = 0;  i < (pRightSize - 1); i++) {
                right.kvInNodeList.add(kvInNodeList.get(pLeftSize + i));
            }
            // Copy the pointer on the right
            for (int i = 0; i < pRightSize; i++) {
                right.childeNodeList.add(childeNodeList.get(pLeftSize + i));
                right.childeNodeList.get(i).parent = right;
            }
            if (false == rootFlag) {// Insert key to parent node of non-leaf node
                int index = parent.childeNodeList.indexOf(this);
                parent.childeNodeList.remove(index);
                parent.childeNodeList.add(index, left);
                parent.childeNodeList.add(index + 1, right);
                left.parent = parent;
                right.parent = parent;
                // Insert key
                parent.kvInNodeList.add(index, keyToParent);
                parent.changeNode(tree);
                kvInNodeList.clear();
                childeNodeList.clear();

                kvInNodeList = null;
                childeNodeList = null;
                parent = null;
            } else {

                rootFlag = false;
                BpNode rootNode = new BpNode(false, true);
                tree.root = rootNode;
                left.parent = rootNode;
                right.parent = rootNode;
                rootNode.childeNodeList.add(left);
                rootNode.childeNodeList.add(right);
                childeNodeList.clear();
                kvInNodeList.clear();
                childeNodeList = null;
                kvInNodeList = null;
                // Insert keywords
                rootNode.kvInNodeList.add(keyToParent);
            }
        }
    }


    private int upper(int x) {
        if (x%2 == 0) {
            return x / 2;
        } else {
            return (x /2 ) + 1;
        }
    }

    private boolean isLeafToSplit() {
        if (kvInNodeList.size() >= (HeapFileConfig.MaxKvCnt - 1)) {
            return true;
        }
        return false;
    }
    private boolean isNodeToSplit() {
        if (childeNodeList.size() > HeapFileConfig.MaxKvCnt) {
            return true;
        }
        return false;
    }

    public int insertKeyCnt =0;
    private void insert(KvInNode key) {
        // insert
        for (int i = 0; i < kvInNodeList.size(); i++) {
            if (kvInNodeList.get(i).wholeMatch(key) == 0) {
                return;
            } else if (kvInNodeList.get(i).wholeMatch(key) > 0) {
                kvInNodeList.add(i, key);
                insertKeyCnt++;
                if(insertKeyCnt%1000000 ==0){
                    System.out.println("");
                }
                return;
            }
        }
        // Insert to the end
        kvInNodeList.add(key);
        insertKeyCnt++;
        if(insertKeyCnt%1000000 ==0){
            System.out.println("");
        }
    }




}
