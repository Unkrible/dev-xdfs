package cn.xzr.xdfs.datanode;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

@Service
public class DatanodeService {

    private final Path rootLocation;

    @Autowired
    public DatanodeService(DatanodeProperties properties) {
        this.rootLocation = Paths.get(properties.getLocation());
    }

    public Path resolve(String filePath){
        return rootLocation.resolve(filePath);
    }

    public Resource loadBlock(String strPath) throws Exception{
        Path filePath = this.resolve(strPath);
        Resource resource = new UrlResource(filePath.toUri());
        if (resource.exists())
            return resource;
        throw new IOException("Block Not Found");
    }

    public void uploadBlock(String filePath, MultipartFile file){
        try (InputStream inputStream = file.getInputStream()){
            Files.copy(inputStream, this.resolve(filePath), StandardCopyOption.REPLACE_EXISTING);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void deleteBlock(String fileName){
        Path filePath = this.resolve(fileName);
        try {
            FileSystemUtils.deleteRecursively(filePath);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void init(){
        try{
            Files.createDirectories(rootLocation);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public void clear(){
        try {
            FileSystemUtils.deleteRecursively(rootLocation);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
