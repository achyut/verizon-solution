#python scrapper.py
hdfs dfs -rm /data.csv
hdfs dfs -copyFromLocal data.csv /
mvn "-Dexec.args=-classpath %classpath com.verizon.Main" -Dexec.executable=/usr/lib/jvm/java-8-oracle/bin/java -Dexec.classpathScope=runtime org.codehaus.mojo:exec-maven-plugin:1.2.1:exec

