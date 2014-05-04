## BigBase source build instructions:

* Copy `settings.xml.template` to `settings.xml` and edit parameters to match your folders

* Use `mvn --settings settings.xml`, it will point to settings.xml to set important properties (native code in lz4 depends on it)
- run the following command to build and install
```
$ mvn --settings settings.xml clean install -DskipTests
```
To create Eclipse enironment files:

```
$ mvn --settings settings.xml eclipse:eclipse -DskipTests
```
> **Note:** All init tests are passed when run in Eclipse one by one, do not run them from maven.
