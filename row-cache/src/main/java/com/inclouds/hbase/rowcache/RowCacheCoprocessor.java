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
package com.inclouds.hbase.rowcache;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Pair;

// TODO: Auto-generated Javadoc
/**
 * The Class ScanCacheCoprocessor.
 * 1. Coprocessor instance is one per table region
 * 2. New instance is created every time the region gets enabled.
 * 3. Call sequence: 
 *     1. new
 *     2. start
 *     3. preOpen 
 *     4. postOpen
 *     
 *     On disable:
 *     1. preClose
 *     2. postClose
 *     3. stop
 * 
 *  Alter Table
 *  
 *  Altering table may:
 *  1. Delete existing CF
 *  2. Modify existing CF
 *  3. Adding new CF
 *  
 *  How does this affect RowCache?
 *  - Deleting existing CF - NO if we HAVE CHECK on preGet that CF exists and cacheable
 *  - Modifying existing CF - TTL and ROWCACHE are of interest. on preOpen we update TTL map, on preGet we check
 *                            if CF exists and is row-cacheable - NO
 *  - adding new CF    - NO at all
 *  
 *   Plan:  
 *   1. on preOpen update only TTL map, 
 *   2. remove preClose, do not do any cache delete operations.
 *   3. keep preBulkLoad. On bulk load table which fully or partially row-cacheable we DELETE all cache
 *   
 *                            
 * 
 */
public class RowCacheCoprocessor extends BaseRegionObserver {


	/** The Constant LOG. */
	  static final Log LOG = LogFactory.getLog(RowCacheCoprocessor.class);	  	
	/** The scan cache. */
	RowCache rowCache;
	
	
	public RowCacheCoprocessor()
	{
		LOG.info("[row-cache] new instance.");
	}
	
	
	public static void resetLicense()
	{

	    RowCache.reset();
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#preClose(org.apache.hadoop.hbase.coprocessor.ObserverContext, boolean)
	 */
	@Override
	public void preClose(ObserverContext<RegionCoprocessorEnvironment> c,
			boolean abortRequested) throws IOException {
       
		LOG.info("[row-cache][preClose] "+c.getEnvironment().getRegion().getRegionInfo().toString());
		// We disabled preClose
		//rowCache.preClose(c.getEnvironment().getRegion().getTableDesc(), abortRequested);
	}



	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#preOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext)
	 */
	@Override
	public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e)
			throws IOException {
		LOG.info("[row-cache][preOpen] "+e.getEnvironment().getRegion().getRegionInfo().toString());
		rowCache.preOpen(e.getEnvironment().getRegion().getTableDesc());
	}

	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#postBulkLoadHFile(org.apache.hadoop.hbase.coprocessor.ObserverContext, java.util.List, boolean)
	 */
	@Override
	public void preBulkLoadHFile(
			ObserverContext<RegionCoprocessorEnvironment> ctx,
			List<Pair<byte[], String>> familyPaths)
			throws IOException {

	    rowCache.preBulkLoadHFile(ctx.getEnvironment().getRegion().getTableDesc(), familyPaths);
	}



	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#preAppend(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Append)
	 */
	@Override
	public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> e,
			Append append) throws IOException {

		  return rowCache.preAppend(e.getEnvironment().getRegion().getTableDesc(), append);
	}



	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#preCheckAndDelete(org.apache.hadoop.hbase.coprocessor.ObserverContext, byte[], byte[], byte[], org.apache.hadoop.hbase.filter.CompareFilter.CompareOp, org.apache.hadoop.hbase.filter.WritableByteArrayComparable, org.apache.hadoop.hbase.client.Delete, boolean)
	 */
	@Override
	public boolean preCheckAndDelete(
			ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
			byte[] family, byte[] qualifier, CompareOp compareOp,
			WritableByteArrayComparable comparator, Delete delete,
			boolean result) throws IOException {

		  return rowCache.preCheckAndDelete(e.getEnvironment().getRegion().getTableDesc(), row, family, qualifier, delete, result);
	}



	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#preCheckAndPut(org.apache.hadoop.hbase.coprocessor.ObserverContext, byte[], byte[], byte[], org.apache.hadoop.hbase.filter.CompareFilter.CompareOp, org.apache.hadoop.hbase.filter.WritableByteArrayComparable, org.apache.hadoop.hbase.client.Put, boolean)
	 */
	@Override
	public boolean preCheckAndPut(
			ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
			byte[] family, byte[] qualifier, CompareOp compareOp,
			WritableByteArrayComparable comparator, Put put, boolean result)
			throws IOException {

		  return rowCache.preCheckAndPut(e.getEnvironment().getRegion().getTableDesc(), row, family, 
				qualifier, put, result);
	}



	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#preDelete(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Delete, org.apache.hadoop.hbase.regionserver.wal.WALEdit, boolean)
	 */
	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
			Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {

	    rowCache.preDelete(e.getEnvironment().getRegion().getTableDesc(), delete);
	}



	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#preIncrement(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Increment)
	 */
	@Override
	public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> e,
			Increment increment) throws IOException {

		  return rowCache.preIncrement(e.getEnvironment().getRegion().getTableDesc(), increment, null);
	}



	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#preIncrementColumnValue(org.apache.hadoop.hbase.coprocessor.ObserverContext, byte[], byte[], byte[], long, boolean)
	 */
	@Override
	public long preIncrementColumnValue(
			ObserverContext<RegionCoprocessorEnvironment> e, byte[] row,
			byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
			throws IOException {
		
		  return rowCache.preIncrementColumnValue(e.getEnvironment().getRegion().getTableDesc(), 
				row, family, qualifier);
	}



	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#prePut(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Put, org.apache.hadoop.hbase.regionserver.wal.WALEdit, boolean)
	 */
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
			Put put, WALEdit edit, boolean writeToWAL) throws IOException 
	{

	    rowCache.prePut(e.getEnvironment().getRegion().getTableDesc(), put);
	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#postGet(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Get, java.util.List)
	 */
	@Override
	public void postGet(ObserverContext<RegionCoprocessorEnvironment> e,
			Get get, List<KeyValue> results) throws IOException 
	{

      rowCache.postGet(e.getEnvironment().getRegion().getTableDesc(), get, results);
    
	}



	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#preExists(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Get, boolean)
	 */
	@Override
	public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> e,
			Get get, boolean exists) throws IOException 
	{

		  boolean result = rowCache.preExists(e.getEnvironment().getRegion().getTableDesc(), get, exists);
		  if( result == true) e.bypass();
		  return result;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#preGet(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Get, java.util.List)
	 */
	@Override
	public void preGet(ObserverContext<RegionCoprocessorEnvironment> e,
			Get get, List<KeyValue> results) throws IOException {
		
		  boolean bypass = rowCache.preGet(e.getEnvironment().getRegion().getTableDesc(), get, results);
		  if(bypass) e.bypass();
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#start(org.apache.hadoop.hbase.CoprocessorEnvironment)
	 */
	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		
		LOG.info("[row-cache][start coprocessor]");
		rowCache = new RowCache();
		rowCache.start(e.getConfiguration());		  
	}


  /* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#stop(org.apache.hadoop.hbase.CoprocessorEnvironment)
	 */
	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {

		  LOG.info("[row-cache][stop coprocessor]");
		  rowCache.stop(e);
		
	}
	

  
  public RowCache getCache()
  {
    return rowCache;
  }

}
