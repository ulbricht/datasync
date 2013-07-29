/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datasync;

import java.io.File;
import java.io.FilenameFilter;

/**
 *
 * @author ulbricht
 */
public class NoHiddenEntityFilter implements FilenameFilter{

    public boolean isFile(File dir, String name){
        File entity=new File(dir.getAbsolutePath()+File.separator+name);
        return entity.isFile();
    }    
    public boolean isDirectory(File dir, String name){
        File entity=new File(dir.getAbsolutePath()+File.separator+name);
        return entity.isDirectory();
    }
    
    @Override
    public boolean accept(File dir, String name) {
        
        File file=new File(dir.getPath()+File.separator+name);
        
        if (file.isHidden() || name.startsWith("."))
            return false;
        
        return true;

    }
    
    
}
