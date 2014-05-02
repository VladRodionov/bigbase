## BigBase source build instructions:

* You need to manually add one jar to your `.m2/repository` folder because it is not available on any public maven repo.
- go to `./lib` folder and run  `*-tests.install.sh` script

* Copy `settings.xml.template` to `settings.xml` and edit parameters to match your folders

* Use `mvn --settings setting.xml`, it will point to settings.xml to set important properties (native code in lz4 depends on it)
- run the following command to build and install
```
$ mvn --settings settings.xml clean install -DskipTests
```
To create Eclipse enironment files:

```
$ mvn --settings settings.xml eclipse:eclipse -DskipTests
```
> **Note:** All init tests are passed when run in Eclipse one by one, do not run them from maven.
