package com.xzr.xdfs.namenode.namespace;

import java.util.List;

public class BlockNode {
    private String name;
    private List<String> dataNodes;
    private Integer size;

    public BlockNode(String name, List<String> dataNodes, Integer size){
        this.name = name;
        this.dataNodes = dataNodes;
        this.size = size;
    }

    public String getName(){
        return this.name;
    }

    public List<String> getDataNodes(){
        return this.dataNodes;
    }

    public Integer getSize(){
        return this.size;
    }

    public void setSize(Integer size){
        this.size = size;
    }
}
