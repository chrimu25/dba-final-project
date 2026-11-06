# HBase 2.5 standalone (HMaster + RegionServer + ZooKeeper in one JVM) with Thrift and REST.
# Built on a multi-architecture Java image, so it runs natively on Apple Silicon (arm64).
FROM eclipse-temurin:11-jre
ARG HBASE_VERSION=2.5.10
RUN apt-get update && apt-get install -y --no-install-recommends curl procps && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz \
    | tar -xz -C /opt && ln -s /opt/hbase-${HBASE_VERSION} /opt/hbase
ENV HBASE_HOME=/opt/hbase PATH=/opt/hbase/bin:$PATH HBASE_HEAPSIZE=2G HBASE_MANAGES_ZK=true
COPY hbase-site.xml /opt/hbase/conf/hbase-site.xml
# 16010 master UI | 9090 Thrift (happybase) | 8080 REST | 2181 ZooKeeper
EXPOSE 16010 9090 8080 2181
CMD bash -c "start-hbase.sh && sleep 10 && hbase-daemon.sh start thrift -p 9090 && hbase-daemon.sh start rest -p 8080 && tail -F /opt/hbase/logs/*.log"
