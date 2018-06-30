package com.xzr.xdfs.namenode.namespace;

import java.util.*;

public class FileNode {
    public static final String blockFormat = "%s/%s-%d";
    public enum Type{
        DIRECTORY, FILE;
    }

    protected String name;
    protected FileNode.Type type;
    protected Map<String, FileNode> children;
    protected Map<String, List<String>> blocks;

    public FileNode(String name){
        this.name = name;
        this.type = Type.DIRECTORY;
        this.children = new HashMap<>();
        this.blocks = null;
    }

    public FileNode(String name, FileNode.Type type){
        this.name = name;
        this.type = type;
        this.children = null;
        this.blocks = null;
    }

    public void setType(FileNode.Type type){
        this.type = type;
    }

    public FileNode query(Iterator<String> filePath){
        return this.query(filePath, false);
    }

    public FileNode query(Iterator<String> filePath, boolean isCreated){
        if(filePath.hasNext()){
            String child = filePath.next();
            if(children.containsKey(child)) {
                return children.get(child).query(filePath);
            }
            else if(isCreated){
                FileNode fileNode = new FileNode(child);
                children.put(child, fileNode);
                return fileNode.query(filePath, true);
            }
            else{
                return null;
            }
        }
        else{
            return this;
        }
    }

    public void addBlock(String block, String dataNode){
        if(blocks.containsKey(block)){
            blocks.get(block).add(dataNode);
        }
        else{
            LinkedList<String> dataNodes = new LinkedList<>();
            dataNodes.add(dataNode);
            blocks.put(block, dataNodes);
        }
    }
}
