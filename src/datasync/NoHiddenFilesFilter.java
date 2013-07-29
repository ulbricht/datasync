/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datasync;

import java.io.File;

/**
 *
 * @author ulbricht
 */
public class NoHiddenFilesFilter extends NoHiddenEntityFilter{

    @Override
    public boolean accept(File dir, String name) {
        return isFile(dir,name);
    }
}
