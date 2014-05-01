##BigBase 1.0.0 installation instructions


> **NOTE:** Install software on HBase Master host first.
 
1. Shutdown HBase cluster (If you do an upgrade of HBase to BigBase)
2. Extract BigBase tar file. 
3. Run `install.sh` script. (Note: `HBASE_HOME` MUST be defined)
4. Modify `$HBASE_HOME/conf/hbase-site.xml` to include BigBase specific configuration parameters (see `./conf/hbase-site.xml.template` for the list of parameters and CONFIGURATION manual)
5. Sync `$HBASE_HOME/` across HBase cluster using provided `synccluster.sh`.
6. Start HBase cluster.
   
The installation script:

* copies all needed library jars into `$HBASE_HOME/lib`.
* copies BigBase shell scripts (`rcadmin.sh` and `synccluster.sh`) into `$HBASE_HOME/bin` directory.
* copies BigBase configuration template file into `$HBASE_HOME/conf` directory.
* updates `$HBASE_HOME/bin/hbase` shell script to include specific BigBase jar files (the copy of previous version of hbase script will be created).




##BigBase Row Cache administration

Row cache can be enabled/disabled per `table:column_family`

### To view row cache configuration for a particular `table`:
```  
 $HBASE_HOME/bin/rcadmin.sh status TABLE
```
### To enable row cache for a `table`
```
 $HBASE_HOME/bin/rcadmin.sh enable TABLE
```
### To enable row cache for a `table:cf`
```
 $HBASE_HOME/bin/rcadmin.sh enable TABLE FAMILY
```
### To disable row cache for a `table`
```
 $HBASE_HOME/bin/rcadmin.sh disable TABLE
```
### To disable row cache for a `table:cf`
```
 $HBASE_HOME/bin/rcadmin.sh disable TABLE FAMILY  
```
 
