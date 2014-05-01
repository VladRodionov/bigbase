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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.koda.cache.OffHeapCache;

// TODO: Auto-generated Javadoc
/**
 * The Class OffHeapCacheKeySet.
 */
public class OffHeapCacheKeySet implements Set<Object> {

	/** The m cache. */
	final OffHeapCache mCache;
		
	/**
	 * Instantiates a new off heap cache key set.
	 *
	 * @param cache the cache
	 */
	OffHeapCacheKeySet(OffHeapCache cache)
	{
		mCache = cache;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Set#add(java.lang.Object)
	 */
	@Override
	public boolean add(Object e) {
		throw new UnsupportedOperationException("add is not supported");
	}

	/* (non-Javadoc)
	 * @see java.util.Set#addAll(java.util.Collection)
	 */
	@Override
	public boolean addAll(Collection<? extends Object> c) {
		throw new UnsupportedOperationException("addAll is not supported");
	}

	/* (non-Javadoc)
	 * @see java.util.Set#clear()
	 */
	@Override
	public void clear() {
		throw new UnsupportedOperationException("clear is not supported");
		
	}

	/* (non-Javadoc)
	 * @see java.util.Set#contains(java.lang.Object)
	 */
	@Override
	public boolean contains(Object key) {		
		try {
			return mCache.containsKey(key);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
	}

	/* (non-Javadoc)
	 * @see java.util.Set#containsAll(java.util.Collection)
	 */
	@Override
	public boolean containsAll(Collection<?> c) {
		
		for(Object key: c){
			try {
				if(!mCache.containsKey(key)) return false;
			} catch (Exception e) {
				throw new RuntimeException(e);
			} 
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see java.util.Set#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		
		return mCache.size() == 0;
	}

	/* (non-Javadoc)
	 * @see java.util.Set#iterator()
	 */
	@Override
	public Iterator<Object> iterator() {
		return new OffHeapCacheKeyIterator(mCache);
	}

	/* (non-Javadoc)
	 * @see java.util.Set#remove(java.lang.Object)
	 */
	@Override
	public boolean remove(Object key) {
		throw new UnsupportedOperationException("remove on immutable collection is not supported");
	}

	/* (non-Javadoc)
	 * @see java.util.Set#removeAll(java.util.Collection)
	 */
	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("removeAll on immutable collection is not supported");
	}

	/* (non-Javadoc)
	 * @see java.util.Set#retainAll(java.util.Collection)
	 */
	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("retainAll on immutable collection is not supported");
	}

	/* (non-Javadoc)
	 * @see java.util.Set#size()
	 */
	@Override
	public int size() {

		return (int)mCache.size();
	}

	/* (non-Javadoc)
	 * @see java.util.Set#toArray()
	 */
	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException("toArray is not supported");
	}

	/* (non-Javadoc)
	 * @see java.util.Set#toArray(T[])
	 */
	@Override
	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException("toArray(T[] a) is not supported");
	}

}
