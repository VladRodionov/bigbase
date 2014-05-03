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
package com.inclouds.hbase.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class RegionServerPoker {
    /** The Constant LOG. */
    static final Log LOG = LogFactory.getLog(RegionServerPoker.class);	
    private static byte[] TABLE = "usertable".getBytes();
    
	private static String THREADS = "-t";
	private static String OPS     = "-ops";
	private static String BATCH   = "-b";
	private static String REUSE_CONFIG = "-rc";
	private static String SEED    = "-s"; 	
	private static String REGION_SERVER = "-rs";
	
	private static int threads = 8;
	private static int ops     = 100000;
	private static int batchSize = 1;
	
	private static AtomicLong  startTime = new AtomicLong(0);
	private static AtomicLong  endTime   = new AtomicLong(0);
	private static AtomicLong  failed = new AtomicLong(0);
	private static AtomicLong  completed = new AtomicLong(0);
	private static AtomicLong  totalSize = new AtomicLong(0);
	private static Timer timer ;
	private static boolean reuseConfig = false;
	private static boolean seedSpecified = false;
	private static int seed;
	private static String regionServer;
	
	static class Stats extends TimerTask
	{

		
		@Override
		public void run() {
			long start = startTime.get();
			
			if(start > 0){
				long current = System.currentTimeMillis();
				LOG.info(((double) completed.get() * 1000)/ (current - start)+" RPS. failed="+failed.get()+
						" Avg size="+ (totalSize.get()/completed.get()));
			}
			
		}
		
	}
	
	static class Poker extends Thread
	{
		Configuration cfg;
		List<byte[]> keys;
		public Poker(int id, List<byte[]> keys)
		{
			super("poker-no-reuse#"+id);
			this.cfg =  HBaseConfiguration.create();
			this.keys = keys;
			
		}
		
		public Poker(int id, Configuration cfg, List<byte[]> keys)
		{
			super("poker-reuse#"+id);
			this.cfg =  cfg;
			this.keys = keys;
			
		}
		
		public void run()
		{
			LOG.info(Thread.currentThread().getName()+" starting ...");
			
			try {
				HTable table = new HTable(cfg, TABLE);
				LOG.info(Thread.currentThread().getName()+" acquired HTable instance and started.");
				startTime.compareAndSet(0, System.currentTimeMillis());
				int counter = 0;
				while(counter ++ < ops){
					List<Get> gets = createBatch();
					Result[] r = table.get(gets);
					checkFailed(r);
					completed.addAndGet(batchSize);
					for(Result res: r){
//						List<KeyValue> kvs = res.list();
//						KeyValue kv = kvs.get(0);
//						int size = kv.getLength();
//						LOG.info("Total kvs ="+ kvs.size()+ " kv size="+size+
//								" f="+kv.getFamilyLength()+" c="+ kv.getQualifierLength()+" ts="+8+" v="+kv.getValueLength()+" row="+kv.getRowLength());
						
						totalSize.addAndGet(getLength(res));
					}
				}
				LOG.info(Thread.currentThread().getName()+" finished");
				endTime.set(System.currentTimeMillis());
			} catch (IOException e) {
				LOG.error(Thread.currentThread().getName(), e);
			}
			
		}
		/**
		 * This is approximate length
		 * 
		 * @param r
		 * @return length 
		 */
		private int getLength(Result r)
		{
			int len =0;
			for(Cell c: r.listCells()){
				len += c.getFamilyLength() + c.getQualifierLength() + c.getRowLength() + c.getValueLength();
			}
			return len;
		}
		private void checkFailed(Result[] r) {			
			for(Result res: r){
				if(res.isEmpty()) failed.incrementAndGet();
			}			
		}

		private List<Get> createBatch() {
			List<Get> gets = new ArrayList<Get>();
			for(int i=0; i < batchSize; i++){
				gets.add(new Get(keys.get(i)));
			}
			return gets;
		}
	}
	
	public static void main(String[] args) throws IOException{
		parseArgs(args);
		
		byte[] name = "usertable".getBytes();
		Configuration cfg = HBaseConfiguration.create();
		
		
		HTable table = new HTable(cfg, name);
		List<byte[]> keys =  selectRandomKeys(table);
		
		LOG.info("Found keys:\n");
		for(byte[] k: keys){
			LOG.info(new String(k));
		}
		
		Poker[] workers = new Poker[threads];
		for(int i=0; i < threads; i++){
			workers[i] = (reuseConfig == false)?new Poker(i, keys): new Poker(i, cfg, keys);
			workers[i].start();
		}
		
		// Start stats
		timer = new Timer();
		timer.schedule( new Stats(), 5000, 5000);
		// Join all workers
		for(int i =0; i < threads; i++){
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
		}
		
		LOG.info("Finished: "+((double) completed.get() * 1000)/ (endTime.get() - startTime.get())+" RPS");
		System.exit(-1);
	}


	private static List<byte[]> selectRandomKeys(HTable table) throws IOException {

		NavigableMap<HRegionInfo, ServerName> map = table.getRegionLocations();		
		ServerName sn = null;
		Set<HRegionInfo> regions =null;
		if( seedSpecified == false && regionServer == null){
			regions = map.keySet();

		} else if( seedSpecified ){
			// select one RS
			sn = getServerName(map, seed);
			LOG.info("Selected "+sn);
			
		} else if( regionServer != null){
			sn = findServerName(map, regionServer);
			LOG.info("Selected "+sn);
		}
		
		if( sn == null && (seedSpecified || regionServer != null)){
			LOG.fatal("Could not select Region Server. Aborted.");
			System.exit(-1);
		}
		
		if(regions == null){
			regions = regionsForServer(map, sn);
		}
		
		byte[] key = null;
		while(key == null){			
			HRegionInfo rInfo = select(regions);
			key = rInfo.getStartKey();
		}
		
		List<byte[]> keys = new ArrayList<byte[]>();
		Scan scan = new Scan(key);
		ResultScanner scanner = table.getScanner(scan);
		int c = 0;
		while( c ++ < batchSize){
			Result r = scanner.next();
			if( r.isEmpty() ){
				LOG.error("Scanner result is empty");
			} else{
				keys.add(r.getRow());
			}
		}
		return keys;
		
	}

    private static Set<HRegionInfo> regionsForServer(
		NavigableMap<HRegionInfo, ServerName> map, ServerName sn) 
	{
    	
    	Set<Map.Entry<HRegionInfo, ServerName>> entries = map.entrySet();
    	Set<HRegionInfo> infos = new HashSet<HRegionInfo>();
    	LOG.info("Regions for "+sn);
    	for( Map.Entry<HRegionInfo, ServerName> entry: entries){
    		ServerName s = entry.getValue();
    		if( s.equals(sn)){
    			HRegionInfo region = entry.getKey();
    			LOG.info("Adding region:"+region);
    			infos.add(region);
    		}
    	}
    	return infos;
	}


	private static ServerName findServerName(
		NavigableMap<HRegionInfo, ServerName> map, String regionServer) 
    {
	   	Collection<ServerName> servers = map.values();
    	for(ServerName sn: servers){
    		LOG.info(sn);
    		if(sn.getHostname().equals(regionServer) )
    			return sn;
    	}
    	return null;
    }


	private static ServerName getServerName(
		NavigableMap<HRegionInfo, ServerName> map, int seed) 
    {
    	Collection<ServerName> servers = map.values();
    	List<ServerName> list = new ArrayList<ServerName>();
    	for(ServerName sn: servers){
    		list.add(sn);
    	}
    	
    	Collections.sort(list, new Comparator<ServerName>(){
			@Override
			public int compare(ServerName arg0, ServerName arg1) {
				return arg0.getHostname().compareTo(arg1.getHostname());
			}
    		
    	});
    	
    	Random r = new Random(seed);
    	int index = r.nextInt(list.size());
    	
    	return list.get(index);
    }


	/**
     * Selects random region
     * @param set
     * @return
     */
	private static HRegionInfo select(Set<HRegionInfo> set) {
		Random r = new Random();
		int i = r.nextInt(set.size());
		return (HRegionInfo)set.toArray()[i];

	}


	private static void parseArgs(String[] args) {
	      try{
	            
	            for(int i=0; i < args.length; i++)
	            {   
	                String arg= args[i];
	                if(arg.equals(THREADS)){  
	                    threads  = Integer.parseInt(args[++i]);	                    	                    
	                } else if(arg.equals(OPS)){
	                    ops = Integer.parseInt(args[++i]);
	                } else if(arg.equals(BATCH)){
	                    batchSize = Integer.parseInt(args[++i]);
	                } else if(arg.equals(REUSE_CONFIG)){
	                    reuseConfig = true;
	                } else if(arg.equals(SEED)){
	                    seedSpecified = true;
	                	seed = Integer.parseInt(args[++i]);
	                } else if(arg.equals(REGION_SERVER)){
	                	regionServer = args[++i];
	                } else{
	                    LOG.error("Unrecognized argument: "+arg);
	                    System.exit(-1);
	                }
	            }               
	            
	        }catch(Exception e){
	            LOG.error("Wrong input arguments", e);
	           
	            System.exit(-1);
	        }
	        
	        LOG.info("Threads      =" + threads);
	        LOG.info("Operations   =" + ops);
	        LOG.info("Batch size   =" + batchSize);
	        LOG.info("Reuse config =" + reuseConfig);
	        LOG.info("Seed         =" + seed);
	        LOG.info("RegionServer =" + regionServer);
		
	}
}
