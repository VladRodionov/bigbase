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
package com.koda.common.locks;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
/**
 * Lock support using busy wait with
 * almost exponential back off.
 * @author vrodionov
 * TODO 1. lock with timeout
 *      2. tryLock with timeout
 *      3. Real exponential backoff
 *      4. Thread.sleep(0) or Thread.yield() strategy
 *       
 * @param <T> - class to support
 */
public class SpinLockControl<T> implements LockControl<T>{

    private final static int LOCKED = 1;
    private final static int UNLOCKED = 0;
    
    private AtomicIntegerFieldUpdater<T> updater;
    
    /** The BACKOF f_ min. */
    private long backoffMin= 20;
    
    /** The BACKOF f_ max. */
    private long backoffMax = 20000;
    
    /** The BACKOF f_ int. */
    private long backoffInt = 20;
    
    /** The BACKOF f_ mi n_ sleep. */
    private long backoffMinSleep = 1000; // ns - 1microsec
    
    /** The BACKOF f_ ma x_ sleep. */
    private long backoffMaxSleep = 1000000;// ns - 1 millisec
    
    /** The BACKOF f_ in t_ sleep. */
    private long backoffSleepInt = 1000;
    
    public SpinLockControl(AtomicIntegerFieldUpdater<T> updater){
            
        this.updater = (AtomicIntegerFieldUpdater<T>) updater;
        
    }
    
    public boolean lock(T obj)
    {
        long count = backoffMin;
        long timeout = backoffMinSleep;
        while(!updater.compareAndSet(obj, UNLOCKED, LOCKED) ){

            if(count < backoffMax){
                int counter =0;
                while(counter++ < count);
                count+= backoffInt;
            } else{
                count = backoffMin;

                if(timeout > backoffMaxSleep){
                    return false;
                } else{
                    long millis = timeout/1000000;
                    long nanos = timeout - millis*1000000;
                    try {                       
                        Thread.sleep(millis, (int)nanos);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        //e.printStackTrace();
                    }
                    timeout+= backoffSleepInt;
                }
            }
            
        }
        return true;
    }
    
    @Override
    public boolean tryLock(T obj) {       
        return updater.compareAndSet(obj, UNLOCKED, LOCKED);
    }

    @Override
    public boolean tryLock(T obj, long timeOut /*nanoseconds*/) {
        long count = backoffMin;
        long timeout = backoffMinSleep;
 
        long stopTime =  System.nanoTime() + timeOut;
        
        while(!updater.compareAndSet(obj, UNLOCKED, LOCKED) ){

            if(count < backoffMax){
                int counter =0;
                while(counter++ < count);
                count+= backoffInt;
            } else{
                count = backoffMin;

                if(timeout > backoffMaxSleep){
                    return false;
                } else{
                    long millis = timeout/1000000;
                    long nanos = timeout - millis*1000000;
                    try {                       
                        Thread.sleep(millis, (int)nanos);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        //e.printStackTrace();
                    }
                    timeout+= backoffSleepInt;
                }
            }
            if(System.nanoTime() > stopTime){
                return false;
            }
        }
        return true;
    }    
    /**
     * Unlock.
     */
    public void unlock(T obj)
    {
        updater.set(obj, UNLOCKED);
    }

    public long getBackoffMin() {
        return backoffMin;
    }

    public void setBackoffMin(long backoffMin) {
        this.backoffMin = backoffMin;
    }

    public long getBackoffMax() {
        return backoffMax;
    }

    public void setBackoffMax(long backoffMax) {
        this.backoffMax = backoffMax;
    }

    public long getBackoffInt() {
        return backoffInt;
    }

    public void setBackoffInt(long backoffInt) {
        this.backoffInt = backoffInt;
    }

    public long getBackoffMinSleep() {
        return backoffMinSleep;
    }

    public void setBackoffMinSleep(long backoffMinSleep) {
        this.backoffMinSleep = backoffMinSleep;
    }

    public long getBackoffMaxSleep() {
        return backoffMaxSleep;
    }

    public void setBackoffMaxSleep(long backoffMaxSleep) {
        this.backoffMaxSleep = backoffMaxSleep;
    }

    public long getBackoffSleepInt() {
        return backoffSleepInt;
    }

    public void setBackoffSleepInt(long backoffSleepInt) {
        this.backoffSleepInt = backoffSleepInt;
    }

    @Override
    public boolean waitUntil( T obj, int value) {
        long count = backoffMin;
        long timeout = backoffMinSleep;
        while( !updater.compareAndSet(obj, value, value) ){

            if(count < backoffMax){
                int counter =0;
                while(counter++ < count);
                count+= backoffInt;
            } else{
                count = backoffMin;

                if(timeout > backoffMaxSleep){
                    return false;
                } else{
                    long millis = timeout/1000000;
                    long nanos = timeout - millis*1000000;
                    try {                       
                        Thread.sleep(millis, (int)nanos);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        //e.printStackTrace();
                    }
                    timeout+= backoffSleepInt;
                }
            }
            
        }
        return true;
        
    }

    
}
