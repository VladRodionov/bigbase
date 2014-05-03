#!/bin/bash
# Rsync's HBase config files across all region servers. Must run on master.
if [ -z "${HBASE_HOME}" ]; then
  echo " HBASE_HOME is not defined. Aborted"
  exit 1
fi

for srv in $(cat ${HBASE_HOME}/conf/regionservers); do
  echo "Sending command to $srv"
  echo "Sync ./conf directory"
  rsync -vaz --delete $HBASE_HOME/conf $srv:$HBASE_HOME
  echo "Sync ./lib directory"
  rsync -vaz --delete $HBASE_HOME/lib $srv:$HBASE_HOME
  echo "Sync ./bin directory"
  rsync -vaz --delete $HBASE_HOME/bin $srv:$HBASE_HOME
done
echo "done."
