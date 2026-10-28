# syntax=docker/dockerfile:1

FROM maven:3.9.11-eclipse-temurin-25 AS build
WORKDIR /workspace

COPY pom.xml ./
COPY src ./src

# Build a Spring Boot executable jar.
RUN mvn --batch-mode -DskipTests package

FROM eclipse-temurin:25-jre
WORKDIR /app

COPY --from=build /workspace/target/recceiver-*.jar /app/recceiver.jar

# Container-friendly defaults; can be overridden by environment variables.
ENV TCP_BIND_ADDRESS=0.0.0.0 \
    TCP_PORT=5064 \
    UDP_BROADCAST_PORT=5049

EXPOSE 5064/tcp
EXPOSE 5049/udp

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/recceiver.jar"]

