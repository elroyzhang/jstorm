=====Eclipse====
mvn eclipse:clean
mvn eclipse:eclipse

=====Package=====
mvn clean 
mvn package assembly:assembly  -Dmaven.test.skip=true

