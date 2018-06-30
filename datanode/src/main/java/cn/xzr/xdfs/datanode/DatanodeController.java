package cn.xzr.xdfs.datanode;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import java.util.logging.Logger;

@RestController
public class DatanodeController {
    @Autowired
    protected DatanodeService datanodeService;
//    protected Logger logger = Logger.getLogger(DatanodeController.class.getName());

    @GetMapping(value = "/test")
    public String getTest(){
        return "Hello";
    }

    @GetMapping(value="/**/{fileName}")
    @ResponseBody
    public ResponseEntity<Resource> downloadBlock(HttpServletRequest request,
                                                  @PathVariable("fileName") String fileName) throws Exception{
        String directory = request.getRequestURI();
        String filePath = directory + "/" + fileName;
        Resource block = datanodeService.loadBlock(filePath);
        return ResponseEntity.ok().header(HttpHeaders.CONTENT_DISPOSITION, fileName).body(block);
    }

    @DeleteMapping(value="/**/{fileName}")
    @ResponseBody
    public ResponseEntity<?> deleteBlock(HttpServletRequest request, @PathVariable("fileName") String fileName){
        String directory = request.getRequestURI();
        String filePath = directory + "/" + fileName;
        datanodeService.deleteBlock(filePath);
        return ResponseEntity.ok().build();
    }

    @PostMapping(value="/")
    public Boolean uploadBlock(HttpServletRequest request,
                                         @RequestParam("block") MultipartFile file){
        String directory = request.getRequestURI();
        datanodeService.uploadBlock(directory, file);
        return true;
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<?> handleException(Exception e){
        return ResponseEntity.noContent().build();
    }
}
