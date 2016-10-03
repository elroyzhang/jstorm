## JDK

```
jdk1.8.0_45
```

## Eclipse

```
mvn eclipse:clean
mvn eclipse:eclipse
```
* import codetemplates 

```windows-->preference-->Java-->Code Style-->Code Templates--> import ```

 选择 ```jstorm-core/src/test/resources/eclipse-java-codetemplates.xml```
 
 需要格式化注释的地方，```alt+shift+j``` 生成注释

* import java code formatter

```windows-->preference-->Java-->Code Style-->Formatter--> import ```

 选择 ```jstorm-core/src/test/resources/jstorm-formatter.xml```

* xml formatter setting

```windows-->preference-->XML-->XML Files-->Editor ```
 
Formatting: Line Width值为72，勾选Clear all lank ines/Format Comments/Join Lines/Insert white spaces before closing empty end-tags，选中Indent using spaces，Indentation size值为1

* import javascript formatter setting

```windows-->preference-->JavaScript-->Code Style-->Formatter--> import ```

 选择 ```jstorm-core/src/test/resources/jstorm-js-formatter.xml```
 
* xml formatter setting

```windows-->preference-->Web-->Html Files-->Editor ```
 
Formatting: Line Width值为72，勾选Clear all lank ines，选中Indent using spaces，Indentation size值为1
 
            
## Package
```
mvn clean -P dist
mvn install -Dmaven.test.skip=true
mvn package -Dmaven.test.skip=true -Dversion.id=38645
mvn package -Dmaven.test.skip=true -P dist -Dwar

```
## How to Release To Maven
1:
```
git tag tdh1u2

git push origin tdh1u2
```
2:
```
 mvn release:clean
 mvn release:update-versions
```
3:
```
 mvn  release:branch -DbranchName=jstorm-20140609
```
4:
```
 mvn release:prepare -DdryRun -Darguments="-DskipTests"

 mvn release:rollback
 mvn release:clean
 mvn clean 
```
5:
```
 mvn release:prepare  -Darguments="-DskipTests"
 mvn release:perform  -Darguments="-DskipTests"
```
## Local Dev Env
* How To Deploy Zookeeper Server

```
cd ~/soft/
wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz  .
tar -zxvf zookeeper-3.4.6.tar.gz

vim ~/.bashrc
export JAVA_HOME=~/software/jdk-current
export JRE_HOME=$JAVA_HOME/jre
export TOMCAT_HOME=~/software/apache-tomcat
export CATALINA_HOME=$TOMCAT_HOME
export MVN_HOME=~/software/apache-maven-3.2.3
export STORM_HOME=~/deploy/storm-current
export ZOOKEEPER_HOME=~/software/zookeeper-3.4.6
export HADOOP_HOME=~/software/hadoop-2.2.0
export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$CATALINA_HOME/bin:$MVN_HOME/bin:$STORM_HOME/bin:$ZOOKEEPER_HOME/bin:$HADOOP_HOME/bin:$PATH

cd $ZOOKEEPER_HOME/conf
cp zoo_sample.cfg  zoo.cfg 
vim zoo.cfg 
tickTime=2000
initLimit=10
syncLimit=5
clientPort=2182
maxClientCnxns=120
autopurge.purgeInterval=24
```

发布 release包

```
mvn deploy:deploy-file -Durl=http://10.130.72.155:8080/nexus/content/repositories/releases -DrepositoryId=tdwmirror -Dfile=./target/jstorm-core-tdh1u1-SNAPSHOT.jar  -DpomFile=./pom.xml -DgroupId=com.tencent.jstorm   -DartifactId=jstorm-core  -Dversion=tdh1u1-SNAPSHOT  -Dpackaging=jar -DgeneratePom=true
```

发布snapshot包

```
mvn deploy:deploy-file -Durl=http://10.130.72.155:8080/nexus/content/repositories/snapshots/  -DrepositoryId=tdwmirror -Dfile=./target/jstorm-core-tdh1u1-SNAPSHOT.jar  -DpomFile=./pom.xml -DgroupId=com.tencent.jstorm   -DartifactId=jstorm-core  -Dversion=tdh1u1-SNAPSHOT  -Dpackaging=jar -DgeneratePom=true
```
