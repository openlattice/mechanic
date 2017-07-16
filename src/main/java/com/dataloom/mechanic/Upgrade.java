package com.dataloom.mechanic;

import java.util.SortedSet;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public interface Upgrade {

    public SortedSet<Integer> getSupportedVersions();
    public int getVersion();
}
