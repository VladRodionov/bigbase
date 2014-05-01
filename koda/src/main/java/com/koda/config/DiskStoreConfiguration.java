/*******************************************************************************
* Copyright (c) 2013 Vladimir Rodionov. All Rights Reserved
*
* This code is released under the GNU Affero General Public License.
*
* See: http://www.fsf.org/licensing/licenses/agpl-3.0.html
*
* VLADIMIR RODIONOV MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY
* OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
* IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR
* NON-INFRINGEMENT. Vladimir Rodionov SHALL NOT BE LIABLE FOR ANY DAMAGES SUFFERED
* BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING THIS SOFTWARE OR
* ITS DERIVATIVES.
*
* Author: Vladimir Rodionov
*
*******************************************************************************/
package com.koda.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.koda.cache.eviction.EvictionPolicy;
import com.koda.common.util.Utils;
import com.koda.compression.CodecType;
import com.koda.persistence.DiskStore;
import com.koda.persistence.PersistenceMode;

// TODO: Auto-generated Javadoc
/**
 * The Class DataStoreConfiguration base class for all DiskStore
 * implementations.
 * 
 */
public class DiskStoreConfiguration implements Cloneable, Defaults{

	/** "none", "ondemand", "snapshots", "write_through", "write_back", "write_around"  ondenamd and snapshots are RawFSStore modes others - LevelDBStore. */
	
	public final static String PERSISTENCE = "koda.store.persistence";
	
	/** The Constant DATA_DIRS. */
	public final static String DATA_DIRS = "koda.store.data.dirs";
	
	/** in seconds. */
	public final static String SNAPSHOT_INTERVAL = "koda.store.snapshot.interval";
	
	/** 'none', 'snappy'. */
	public final static String COMPRESSION = "koda.store.compression";
	
	/** The Constant MAX_SIZE. */
	public final static String STORE_SIZE_LIMIT = "koda.store.max.size";
	
	/** The Constant EVICTION_HIGHWATERMARK. */
	public final static String EVICTION_HIGHWATERMARK = "koda.store.eviction.highwatermark";
	
	/** The Constant EVICTION_LOWWATERMARK. */
	public final static String EVICTION_LOWWATERMARK = "koda.store.eviction.lowwatermark";
	
	/** The Constant EVICTION_POLICY. */
	public final static String EVICTION_POLICY = "koda.store.eviction.policy";
	
	/** The Constant IMPL_CLASSNAME. */
	public final static String IMPL_CLASSNAME = "koda.store.impl.classname";
	
	/** The Constant NAME. */
	public final static String NAME = "koda.store.name";
	
	public final static String EVICTION_BACKGROUND = "koda.store.eviction.background";
	
	public final static String OVERFLOW_TO_DISK = "koda.store.overflow.to.disk";
	
	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(DiskStoreConfiguration.class);	
	/** DataStore - related configuration. */
	
	public final static String DEFAULT_PERSISTENCE = "NONE";
	
	public final static String DEFAULT_DATA_DIRS = ".";
	
	public final static String DEFAULT_SNAPSHOT_INTERVAL = "3600000";
	
	public final static String DEFAULT_COMPRESSION = "NONE";
	
	public final static String DEFAULT_STORE_SIZE = DEFAULT_UNLIMITED;
	
	public final static String DEFAULT_NAME = "store";
	
	
	
	
	/** Disk store max size. Max size is applied to ALL volumes. If we have N volumes
	 * and maxSize = M, then max size of each volume is M/N 
	 * if =0, then there is no limit and no eviction
	 **/
	

	/** The disk store implementation. */
	transient protected Class<? extends DiskStore> diskStoreImplementation;
	
	
	private Properties mProperties = new Properties();
	/**
	 * Instantiates a new disk store configuration.
	 */
	public DiskStoreConfiguration(){}
	
	/**
	 * Gets the persistence type.
	 *
	 * @return the persistence type
	 */
	public PersistenceMode getPersistenceMode()
	{
		return PersistenceMode.valueOf(mProperties.getProperty(PERSISTENCE, DEFAULT_PERSISTENCE));
	}
	
	/**
	 * Sets the persistence type.
	 *
	 * @param type the new persistence type
	 */
	public void setPersistenceMode(PersistenceMode type)
	{
		mProperties.setProperty(PERSISTENCE, type.name());
	}

	/**
	 * Gets the db data store roots. This can be the list
	 * of directories (volumes)
	 *
	 * @return the db data store roots
	 */
	public String[] getDbDataStoreRoots() {
		String roots = mProperties.getProperty(DATA_DIRS, DEFAULT_DATA_DIRS);
	    return roots.split(",");
	}

	/**
	 * Sets the db data store roots.
	 *
	 * @param dbDataStoreRoots the new db data store roots
	 */
	public void setDbDataStoreRoots(String[] roots) {
	    mProperties.setProperty(DATA_DIRS, Utils.concat(roots, ","));
	}

	/**
	 * Gets the db snapshot interval.
	 *
	 * @return the db snapshot interval
	 */
	public long getDbSnapshotInterval() {
		return Long.parseLong(mProperties.getProperty(SNAPSHOT_INTERVAL, DEFAULT_SNAPSHOT_INTERVAL));
	}

	/**
	 * Sets the db snapshot interval.
	 *
	 * @param dbSnapshotInterval the new db snapshot interval
	 */
	public void setDbSnapshotInterval(long interval) {
		mProperties.setProperty(SNAPSHOT_INTERVAL, Long.toString(interval));
	}



	/**
	 * Gets the db compression type.
	 *
	 * @return the db compression type
	 */
	public CodecType getDbCompressionType() {
		return CodecType.valueOf(mProperties.getProperty(COMPRESSION, "NONE"));
	}

	/**
	 * Sets the db compression type.
	 *
	 * @param dbCompressionType the new db compression type
	 */
	public void setDbCompressionType(CodecType type) {
	    mProperties.setProperty(COMPRESSION, type.name());
	}

	/**
	 * Gets the disk store max size.
	 *
	 * @return the disk store max size
	 */
	public long getDiskStoreMaxSize() {
		return Long.parseLong(mProperties.getProperty(STORE_SIZE_LIMIT, DEFAULT_UNLIMITED));
	}

	/**
	 * Sets the disk store max size.
	 *
	 * @param diskStoreMaxSize the new disk store max size
	 */
	public void setDiskStoreMaxSize(long diskStoreMaxSize) {
	    mProperties.setProperty(STORE_SIZE_LIMIT, Long.toString(diskStoreMaxSize));
	}

	/**
	 * Gets the disk store eviction high watermark.
	 *
	 * @return the disk store eviction high watermark
	 */
	public float getDiskStoreEvictionHighWatermark() {
		return Float.parseFloat(mProperties.getProperty(EVICTION_HIGHWATERMARK, DEFAULT_HIGH_WATERMARK));
	}

	/**
	 * Sets the disk store eviction high watermark.
	 *
	 * @param diskStoreEvictionHighWatermark the new disk store eviction high watermark
	 */
	public void setDiskStoreEvictionHighWatermark(
			float diskStoreEvictionHighWatermark) {
	    mProperties.setProperty(EVICTION_HIGHWATERMARK, Float.toString(diskStoreEvictionHighWatermark));
	}

	/**
	 * Gets the disk store eviction eviction low watermak.
	 *
	 * @return the disk store eviction eviction low watermak
	 */
	public float getDiskStoreEvictionLowWatermak() {
	    return Float.parseFloat(mProperties.getProperty(EVICTION_LOWWATERMARK, DEFAULT_LOW_WATERMARK));
	}

	/**
	 * Sets the disk store eviction eviction low watermak.
	 *
	 * @param diskStoreEvictionEvictionLowWatermak the new disk store eviction eviction low watermak
	 */
	public void setDiskStoreEvictionLowWatermak(
			float diskStoreEvictionEvictionLowWatermak) {
	    mProperties.setProperty(EVICTION_LOWWATERMARK, Float.toString(diskStoreEvictionEvictionLowWatermak));
	}

	/**
	 * Gets the disk store eviction policy.
	 *
	 * @return the disk store eviction policy
	 */
	public EvictionPolicy getDiskStoreEvictionPolicy() {
		return EvictionPolicy.valueOf(mProperties.getProperty(EVICTION_POLICY, DEFAULT_NONE));
	}

	/**
	 * Sets the disk store eviction policy.
	 *
	 * @param diskStoreEvictionPolicy the new disk store eviction policy
	 */
	public void setDiskStoreEvictionPolicy(EvictionPolicy diskStoreEvictionPolicy) {
		mProperties.setProperty(EVICTION_POLICY, diskStoreEvictionPolicy.name());
	}

	/**
	 * Checks if is eviction background.
	 *
	 * @return true, if is eviction background
	 */
	public boolean isEvictionBackground() {
		return Boolean.parseBoolean(mProperties.getProperty(EVICTION_BACKGROUND, FALSE));
	}

	/**
	 * Sets the eviction background.
	 *
	 * @param isEvictionBackground the new eviction background
	 */
	public void setEvictionBackground(boolean isEvictionBackground) {
	    mProperties.setProperty(EVICTION_BACKGROUND, Boolean.toString(isEvictionBackground));
	}

	/**
	 * Checks if is overflow to disk.
	 *
	 * @return true, if is overflow to disk
	 */
	public boolean isOverflowToDisk() {
	    return Boolean.parseBoolean(mProperties.getProperty(OVERFLOW_TO_DISK, FALSE));
	}

	/**
	 * Sets the overflow to disk.
	 *
	 * @param overflowToDisk the new overflow to disk
	 */
	public void setOverflowToDisk(boolean overflowToDisk) {
	    mProperties.setProperty(OVERFLOW_TO_DISK, Boolean.toString(overflowToDisk));
	}

	/**
	 * Gets the disk store implementation.
	 *
	 * @return the disk store implementation
	 */
	@SuppressWarnings("unchecked")
	public Class<? extends DiskStore> getDiskStoreImplementation() {
		if(diskStoreImplementation == null){
			try {
				diskStoreImplementation = (Class<? extends DiskStore>) Class.forName(getStoreClassName());
			} catch (Exception e) {				
				LOG.fatal("ClassNotFound :"+getStoreClassName());
				throw new RuntimeException(e.fillInStackTrace());
			}
		}
		return diskStoreImplementation;
	}

	/**
	 * Sets the disk store implementation.
	 *
	 * @param diskStoreImplementation the new disk store implementation
	 */
	public void setDiskStoreImplementation(
			Class<? extends DiskStore> diskStoreImplementation) {
		this.diskStoreImplementation = diskStoreImplementation;
		setStoreClassName(diskStoreImplementation.getName());
	}

	/**
	 * Gets the db name.
	 *
	 * @return the db name
	 */
	public String getStoreName() {
		return mProperties.getProperty(NAME, DEFAULT_NAME);
	}

	/**
	 * Sets the db name.
	 *
	 * @param dbName the new db name
	 */
	public void setStoreName(String dbName) {
		mProperties.setProperty(NAME, dbName);
	}

	/**
	 * Gets the store class name.
	 *
	 * @return the store class name
	 */
	public String getStoreClassName() {
		return mProperties.getProperty(IMPL_CLASSNAME, DEFAULT_UNDEFINED);
	}

	/**
	 * Sets the store class name.
	 *
	 * @param storeClassName the new store class name
	 */
	public void setStoreClassName(String storeClassName) {
		mProperties.setProperty(IMPL_CLASSNAME, storeClassName );
	}
	
	/**
	 * Copy.
	 *
	 * @return the disk store configuration
	 * @throws CloneNotSupportedException the clone not supported exception
	 */
	public DiskStoreConfiguration copy() throws CloneNotSupportedException
	{
		return (DiskStoreConfiguration) clone();
	}

    public static DiskStoreConfiguration load(InputStream is) throws IOException {
        DiskStoreConfiguration cfg = new DiskStoreConfiguration();      
        cfg.mProperties.load(is);
        return cfg;
    }

    public void store(OutputStream os, String string) throws IOException {
        LOG.info("store:: Saving disk store configuration");
        mProperties.store(os, "Koda DiskStore configuration. Stored on "+new Date());
        LOG.info("store:: Done");
        
    }

}
