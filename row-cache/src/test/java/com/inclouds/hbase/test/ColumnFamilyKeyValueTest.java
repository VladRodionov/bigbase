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
package com.inclouds.hbase.test;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

// TODO: Auto-generated Javadoc
/**
 * The Class ColumnFamilyKeyValueTest.
 */
public class ColumnFamilyKeyValueTest {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws TableNotFoundException the table not found exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void main(String[] args) throws TableNotFoundException, IOException {
		
		byte[] tableName = "RAINBOW_JAN-IDPROFILES".getBytes();
		
		HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
		HTableDescriptor desc = admin.getTableDescriptor(tableName);
		
		System.out.println("TABLE:\n"+desc);
		
		HColumnDescriptor[] dds  = desc.getColumnFamilies();
		for(HColumnDescriptor d: dds){
			System.out.println(d);
		}
		if(admin.isTableEnabled(tableName)){
			admin.disableTable(tableName);
		}
		
		desc.setValue("ROWCACHE", "true");
		
		dds[0].setValue("ROW_CACHE", "true");		
		admin.modifyColumn(tableName, dds[0]);
		admin.modifyTable(tableName, desc);
		
		System.out.println("Updated CF . Waiting 20 sec");
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Enabling table ");
		admin.enableTable(tableName);
		System.out.println("Enabling table done .");
		desc = admin.getTableDescriptor(tableName);
		dds  = desc.getColumnFamilies();
		System.out.println("TABLE:\n"+desc);
		for(HColumnDescriptor d: dds){
			System.out.println(d);
		}
	}

}
