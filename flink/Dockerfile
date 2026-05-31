FROM flink:1.19.1-scala_2.12-java17

ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar /opt/flink/lib/flink-sql-connector-kafka.jar
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar /opt/flink/lib/flink-connector-jdbc.jar
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/flink/lib/postgresql.jar

RUN chmod 0644 /opt/flink/lib/flink-sql-connector-kafka.jar \
    /opt/flink/lib/flink-connector-jdbc.jar \
    /opt/flink/lib/postgresql.jar
