FROM maven:3.9-eclipse-temurin-21 AS build
ARG MODULE=frontend/ds/server
WORKDIR /build
COPY . .
RUN mvn -pl ${MODULE} -am package -DskipTests -q

FROM eclipse-temurin:21-jre
ARG MODULE=frontend/ds/server
ARG JAR=ds-server.jar
COPY --from=build /build/${MODULE}/target/${JAR} /opt/streamstack/server.jar
WORKDIR /opt/streamstack
ENTRYPOINT ["java", "-jar", "/opt/streamstack/server.jar"]
