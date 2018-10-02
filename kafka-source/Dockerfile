FROM strimzi/java-base:0.7.0

ADD target/kafka-source-0.0.1-SNAPSHOT.jar /

EXPOSE 8081

CMD /bin/launch_java.sh /kafka-source-0.0.1-SNAPSHOT.jar
