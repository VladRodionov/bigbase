#!/usr/bin/env bash
# Install BigBase main script
# Must be run on HBASE_MASTER

version=1.0.0

if [ -z "${HBASE_HOME}" ]; then
  echo " HBASE_HOME is not defined. Aborted"
  exit 1
fi
echo "Copying README.BB, INSTALL.BB, CONFIGURATION.BB, LICENSE.BB to $HBASE_HOME"

cp ../*.BB $HBASE_HOME/

echo "Copying all ./lib files over to $HBASE_HOME/lib"

cp ../lib/* $HBASE_HOME/lib

echo "Copying ./bin/synccluster.sh to $HBASE_HOME/bin"
cp ../bin/synccluster.sh $HBASE_HOME/bin

echo "Copying ./bin/rcadmin.sh.sh to $HBASE_HOME/bin"
cp ../bin/rcadmin.sh $HBASE_HOME/bin

echo "Copying ./conf templates to $HBASE_HOME/conf"
cp ../conf/* $HBASE_HOME/conf

cp $HBASE_HOME/bin/hbase $HBASE_HOME/bin/hbase.bak

for f in $HBASE_HOME/lib/block-cache*; do
   echo "Adding "$f" to CLASSPATH"
   sed  -i.remove 's|CLASSPATH="${HBASE_CONF_DIR}"|CLASSPATH="'$f':${HBASE_CONF_DIR}"|g' $HBASE_HOME/bin/hbase
done

rm $HBASE_HOME/bin/hbase.remove

echo "Use $HBASE_HOME/conf/hbase-site.xml.template and modify BigBase settings, then run $HBASE_HOME/bin/synccluster.sh to sync your cluster nodes."
echo "BigBase-$version installation completed."

