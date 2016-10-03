##Package
* package war

```
mvn clean package -DskipTests=true -Dwar
cp ./target/storm-ui.war $TOMCAT_HOME/webapps/
```

* package jar 

```
mvn clean package -DskipTests=true
cp ./target/storm-ui-*.jar  $STORM_HOME/external/storm-ui/
```
## MUST Config STORM_CONF_DIR

You can specify the storm configuration directory by setting system environment variable. By default,
you can create a new directory named ".storm" in the user home,and set the storm configuration by storm.yaml

* config by setting system environment variable

```    
   export STORM_CONF_DIR
```

* Create a new directory named ".storm" in the user home. Like "~/.storm"

```    
    cd ~
    mkdir .storm
    vim storm.yaml
```

## How to Deploy to Tomcat
* Download apache-tomcat

```
  wget http://mirror.bit.edu.cn/apache/tomcat/tomcat-7/v7.0.68/bin/apache-tomcat-7.0.68.tar.gz
  tar -zvxf apache-tomcat-7.0.68.tar.gz
  ln -s apache-tomcat-7.0.68 apache-tomcat-current
```
* Config Env

```
vi /etc/profile
export TOMCAT_HOME=~/soft/apache-tomcat-current
export CATALINA_HOME=$TOMCAT_HOME
export PATH=$TOMCAT_HOME/bin:$PATH

```
* modify $TOMCAT_HOME/conf/server.xml

```
add "<Context path="" docBase="storm-ui" debug="0"  reloadable="true" crossContext="true"/>" between <host> and </host>. 
Like:
    <Host name="localhost"  appBase="webapps"
            unpackWARs="true" autoDeploy="true">
          
        <!-- SingleSignOn valve, share authentication between web applications
             Documentation at: /docs/config/valve.html -->
        <!--
        <Valve className="org.apache.catalina.authenticator.SingleSignOn" />
        --> 

        <!-- Access log processes all example.
             Documentation at: /docs/config/valve.html 
             Note: The pattern used is equivalent to using pattern="common" -->
        <Valve className="org.apache.catalina.valves.AccessLogValve" directory="logs"
               prefix="localhost_access_log." suffix=".txt"
               pattern="%h %l %u %t &quot;%r&quot; %s %b" />
    
       <Context path="" docBase="storm-ui" debug="0"  reloadable="true" crossContext="true"/>
       
    </Host>
```

* 部署storm-ui.war

```
1. 进入$TOMCAT_HOME/webapps/目录： cd $TOMCAT_HOME/webapps/
2. 备份原有$TOMCAT_HOME/webapps/ROOT目录： mv ROOT ROOTBAK
3. 新建ROOT目录：mkdir ROOT
4. 拷贝storm-ui.war到ROOT目录
5. 在ROOT目录解压storm-ui.war文件：jar -vxf storm-ui.war
```

* modify ./startup.sh & shutdown.sh

```
write  Config Env to ./startup.sh & shutdown.sh
```
* Start TomCat Server

```
# $TOMCAT_HOME/bin/startup.sh
Using CATALINA_BASE:   ~/soft/apache-tomcat-current
Using CATALINA_HOME:   ~/soft/apache-tomcat-current
Using CATALINA_TMPDIR: ~/soft/apache-tomcat-current/temp
Using JRE_HOME:        ~/soft/jdk-current/jre
Using CLASSPATH:       ~/soft/apache-tomcat-current/bin/bootstrap.jar:~/soft/apache-tomcat-current/bin/tomcat-juli.jar
Tomcat started.
```
* Shut Down TomCat Server

```
# $TOMCAT_HOME/bin/shutdown.sh
```

* 访问storm-ui的服务

```
启动之后，浏览器访问：http://ip:port
```
