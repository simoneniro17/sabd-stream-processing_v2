FROM openjdk:17-jdk-slim
WORKDIR /app
COPY target/kafkastreams-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]