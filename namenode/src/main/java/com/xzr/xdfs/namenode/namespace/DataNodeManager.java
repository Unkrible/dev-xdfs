package com.xzr.xdfs.namenode.namespace;

import java.util.*;

public class DataNodeManager {
    protected Map<String, List<String>> dataNodes = new HashMap<>();

    public String getDataNode(){
        Random random = new Random();
        Set<String> dataNodeUrls = dataNodes.keySet();
        int selected = random.nextInt(dataNodeUrls.size());
        Iterator<String> itor = dataNodeUrls.iterator();
        String dataNodeUrl = itor.next();
        for(int i=0; i<selected && itor.hasNext(); ++i)
            dataNodeUrl = itor.next();
        return dataNodeUrl;
    }

    public boolean registerDataNode(String dataNodeUrl){
        if(dataNodes.containsKey(dataNodeUrl)) {
            System.err.println("Repeated DataNode Registered " + dataNodeUrl);
            return false;
        }
        else {
            dataNodes.put(dataNodeUrl, new LinkedList<String>());
            return true;
        }
    }

    public boolean dispatchBlock(String dataNodeUrl, String blockPath){
        if(dataNodes.containsKey(dataNodeUrl)){
            dataNodes.get(dataNodeUrl).add(blockPath);
            return true;
        }
        else
            return false;
    }
}
