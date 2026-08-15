FROM maven:3.9-eclipse-temurin-21 AS build
ARG MODULE=frontend/ds/server
WORKDIR /build
COPY . .
RUN mvn -pl ${MODULE} -am package -DskipTests -q

FROM eclipse-temurin:21-jre
ARG MODULE=frontend/ds/server
ARG JAR=ds-server.jar
COPY --from=build /build/${MODULE}/target/${JAR} /opt/streamstack/server.jar
COPY harness/docker/entrypoint.sh /opt/streamstack/entrypoint.sh
COPY harness/local/topo.yaml /opt/streamstack/topo/local.yaml
COPY harness/local/topo.cluster.yaml /opt/streamstack/topo/local.cluster.yaml
COPY harness/aws/topo.yaml /opt/streamstack/topo/aws.yaml
COPY harness/aws/topo.cluster.yaml /opt/streamstack/topo/aws.cluster.yaml
COPY harness/fly/topo.yaml /opt/streamstack/topo/fly.yaml
COPY harness/fly/topo.cluster.yaml /opt/streamstack/topo/fly.cluster.yaml
WORKDIR /opt/streamstack
RUN chmod +x /opt/streamstack/entrypoint.sh
ENTRYPOINT ["/opt/streamstack/entrypoint.sh"]
