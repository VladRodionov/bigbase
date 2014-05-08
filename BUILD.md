## BigBase source build instructions:

* Copy `settings.xml.template` to `settings.xml` and edit parameters to match your folders

* Use `mvn --settings settings.xml`, it will point to settings.xml to set important properties (native code in lz4 depends on it)
- run the following command to build HBase-0.96.x compatible version of BigBase: 
```
$ mvn --settings settings.xml clean install -DskipTests -Dhbase.version=0.96.2-hadoop2
```
To create Eclipse environment files:

```
$ mvn --settings settings.xml eclipse:eclipse -DskipTests -Dhbase.version=0.96.2-hadoop2
```

> **Note:** All init tests are passed when run in Eclipse one by one, do not run them from maven.

To build HBase 0.98.x - compatible BigBase, set `hbase.version=0.98.1-hadoop2`
