/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.gfz_potsdam.datasync;

import java.io.File;

/**
 *
 * @author ulbricht
 */
public class NoHiddenDirectoriesFilter extends NoHiddenEntityFilter{

    @Override
    public boolean accept(File dir, String name) {
        return isDirectory(dir,name);
    }
}
