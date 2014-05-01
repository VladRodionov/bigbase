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
package com.koda.integ.ispn;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;

import com.koda.NativeMemoryException;
import com.koda.cache.OffHeapCache;

// TODO: Auto-generated Javadoc
/**
 * The Class OffHeapDataContainer.
 */
public class OffHeapDataContainer implements DataContainer {

	/** The m data container. */
	private OffHeapCache mDataContainer;
	
	/** The m entry factory. */
	private InternalEntryFactory mEntryFactory;
	
	/**
	 * Instantiates a new off heap data container.
	 *
	 * @param parent the parent
	 */
	public OffHeapDataContainer(OffHeapCache parent)
	{
		this.mDataContainer = parent;
		this.mEntryFactory = new InternalEntryFactory();
	}
	
	/* (non-Javadoc)
	 * @see org.infinispan.container.DataContainer#clear()
	 */
	@Override
	public void clear() {
		try{
			mDataContainer.clear();
		}catch(NativeMemoryException e){
			throw new RuntimeException(e);
		}

	}

	/* (non-Javadoc)
	 * @see org.infinispan.container.DataContainer#containsKey(java.lang.Object)
	 */
	@Override
	public boolean containsKey(Object k) {
		
		try {
			return mDataContainer.containsKey(k);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.infinispan.container.DataContainer#entrySet()
	 */
	@Override
	public Set<InternalCacheEntry> entrySet() {
		
		return new OffHeapCacheEntrySet(mDataContainer);
	}

	/* (non-Javadoc)
	 * @see org.infinispan.container.DataContainer#get(java.lang.Object)
	 */
	@Override
	public InternalCacheEntry get(Object k) {
		InternalCacheEntry e;
		Object v = null;
		try {
			v = mDataContainer.get(k);
		} catch (Exception e1) {
			throw new RuntimeException(e1);
		} 
		if(v == null) return null;
		long lifespan=0, maxIdle=0;
		e = mEntryFactory.createNewEntry(k, v, lifespan, maxIdle);
		return e;
	}

	/* (non-Javadoc)
	 * @see org.infinispan.container.DataContainer#keySet()
	 */
	@Override
	public Set<Object> keySet() {		
		// every time creates new one
		return new OffHeapCacheKeySet(mDataContainer);
	}

	/* (non-Javadoc)
	 * @see org.infinispan.container.DataContainer#peek(java.lang.Object)
	 */
	@Override
	public InternalCacheEntry peek(Object k) {
		InternalCacheEntry e;
		Object v = null;
		try {
			v = mDataContainer.peek(k);
		} catch (Exception e1) {
			throw new RuntimeException(e1);
		} 
		if(v == null) return null;
		long lifespan=0, maxIdle=0;
		e = mEntryFactory.createNewEntry(k, v, lifespan, maxIdle);
		return e;
	}

	/* (non-Javadoc)
	 * @see org.infinispan.container.DataContainer#purgeExpired()
	 */
	@Override
	public void purgeExpired() {
		// does nothing

	}

	/* (non-Javadoc)
	 * @see org.infinispan.container.DataContainer#put(java.lang.Object, java.lang.Object, long, long)
	 */
	@Override
	public void put(Object k, Object v, long lifespan, long maxIdle) {
		try{
			// we think that lifespan is when it expires
			mDataContainer.put(k, v, lifespan);
		}catch(Exception e){
			throw new RuntimeException(e);
		}

	}

	/* (non-Javadoc)
	 * @see org.infinispan.container.DataContainer#remove(java.lang.Object)
	 */
	@Override
	public InternalCacheEntry remove(Object k) {
		try{
			Object value = mDataContainer.removeValue(k);
			if(value == null) return null;
			InternalCacheEntry en = InternalEntryFactory.create(k, value);
			return en;
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.infinispan.container.DataContainer#size()
	 */
	@Override
	public int size() {		
		return (int) mDataContainer.size();
	}

	/* (non-Javadoc)
	 * @see org.infinispan.container.DataContainer#values()
	 */
	@Override
	public Collection<Object> values() {
		return new OffHeapCacheValueCollection(mDataContainer);
	}

	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<InternalCacheEntry> iterator() {
		return new OffHeapCacheKeyValueIterator(mDataContainer);
	}

	
	
}
