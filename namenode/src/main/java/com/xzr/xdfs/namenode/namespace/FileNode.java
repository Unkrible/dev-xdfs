package com.xzr.xdfs.namenode.namespace;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FileNode {
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
}
