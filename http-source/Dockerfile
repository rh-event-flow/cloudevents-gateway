FROM strimzi/java-base:8-3

ADD target/http-source-0.0.1-SNAPSHOT.jar /

EXPOSE 8081

CMD /bin/launch_java.sh /http-source-0.0.1-SNAPSHOT.jar
