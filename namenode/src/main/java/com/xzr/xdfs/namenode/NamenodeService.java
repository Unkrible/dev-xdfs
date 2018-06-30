package com.xzr.xdfs.namenode;

import com.xzr.xdfs.namenode.namespace.BlockNode;
import com.xzr.xdfs.namenode.namespace.DataNodeManager;
import com.xzr.xdfs.namenode.namespace.FileNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Service
public class NamenodeService {

    private static final String cacheFormat = "%s%s";
    private static final String blockCacheDir = "tmp/blockCache";
    private static final String fileCacheDir = "tmp/fileCache";

    @Autowired
    protected RestTemplate restTemplate;

    protected FileNode fileNodeManager = new FileNode("/");
    protected DataNodeManager dataNodeManager = new DataNodeManager();

    @Value(value="${block.block-size:${block.default-size}}")
    public int BLOCK_SIZE;

    @Value(value="${block.block-replicas:${block.default-replicas}}")
    public int REPLICA_NUM;

    protected Logger logger = Logger.getLogger(NamenodeService.class.getName());

    public void init(){
        new File(blockCacheDir).mkdirs();
        new File(fileCacheDir).mkdirs();
    }

    public String getTest(){
        return restTemplate.getForObject("http://localhost:2222/test/", String.class);
    }

    public Resource downloadFile(String strPath, String fileName){
        List<String> filePath = Arrays.asList(strPath.split("/"));
        FileNode directory = fileNodeManager.query(filePath.iterator());
        int fileSize = 0;
        LinkedList<Byte> blockBytes = new LinkedList<>();
        if(directory == null)
            return null;

        for(BlockNode blockNode : directory.getBlocks()){
            Resource fileResource = null;
            List<String> dataNodes = blockNode.getDataNodes();
            for(String dataNode : dataNodes){
                fileResource = restTemplate.getForEntity(dataNode+blockNode.getName(), Resource.class).getBody();
                if(fileResource == null)
                    continue;
                try {
                    blockBytes.addAll(this.unboxResource(fileResource, blockNode.getSize()));
                    fileSize += blockNode.getSize();
                    break;
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }
        File file = new File(String.format(cacheFormat, blockCacheDir, strPath));
        try {
            Resource resource = this.boxUpByteList(blockBytes, fileSize, file);
            return resource;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public boolean uploadFile(MultipartFile file, String strPath, String fileName){
        List<String> filePath = Arrays.asList(strPath.split("/"));
        FileNode directory = fileNodeManager.query(filePath.iterator(), true);
        directory.setType(FileNode.Type.FILE);
        try{
            long blockNum = (long)Math.ceil((double)file.getSize()/(double)BLOCK_SIZE);
            BufferedInputStream fileStream = new BufferedInputStream(file.getInputStream());
            for(long i=0; i<blockNum; ++i){
                String blockName = String.format(FileNode.blockFormat, strPath, fileName, i);
                byte[] blockBytes = new byte[BLOCK_SIZE];
                int blockSize = fileStream.read(blockBytes);
                File block = new File(String.format(cacheFormat, blockCacheDir, blockName.replaceAll("/", "-")));
                FileSystemResource fileBlockResource = boxUpByteArray(blockBytes, blockSize, block);
                MultiValueMap<String, Object> parameters = new LinkedMultiValueMap<>();
                parameters.add("block", fileBlockResource);
                for(int j=0; j<this.REPLICA_NUM; ++j) {
                    Boolean result = null;
                    String dataNode = "";
                    while (result == null || !result) {
                        dataNode = dataNodeManager.getDataNode();
                        result = restTemplate.postForObject(dataNode, parameters, Boolean.class);
                    }
                    dataNodeManager.dispatchBlock(dataNode, blockName);
                    fileNodeManager.addBlock(blockName, dataNode, blockSize);
                }
                block.delete();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    public boolean deleteFile(String strPath, String fileName){
        List<String> filePath = Arrays.asList(strPath.split("/"));
        FileNode directory = fileNodeManager.query(filePath.iterator());
        if(directory == null)
            return true;

        for(BlockNode blockNode : directory.getBlocks()){
            List<String> dataNodes = blockNode.getDataNodes();
            for(String dataNode : dataNodes){
                restTemplate.delete(dataNode+blockNode.getName());
            }
        }
        return true;
    }

    public boolean registerDataNode(String dataNodeUrl) {
        return dataNodeManager.registerDataNode(dataNodeUrl);
    }

    private FileSystemResource boxUpByteArray(byte[] bytes, int size, File file)throws IOException{
        FileOutputStream writer = new FileOutputStream(file);
        writer.write(bytes, 0, size);
        writer.close();
        return new FileSystemResource(file);
    }

    private FileSystemResource boxUpByteList(List<Byte> bytes, int size, File file)throws IOException{
        byte[] byteArray = new byte[size];
        for(int i=0; i<size; ++i){
            byteArray[i] = bytes.get(i);
        }
        FileOutputStream writer = new FileOutputStream(file);
        writer.write(byteArray, 0, size);
        writer.close();
        return new FileSystemResource(file);
    }

    private List<Byte> unboxResource(Resource resource, int size) throws IOException{
        List<Byte> byteList = new LinkedList<>();
        byte[] bytes = new byte[size];
        FileInputStream reader = new FileInputStream(resource.getFile());
        reader.read(bytes, 0, size);
        for(byte each : bytes){
            byteList.add(each);
        }
        return byteList;
    }
}
