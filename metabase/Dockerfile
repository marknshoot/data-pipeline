# Metabase does not bundle a ClickHouse driver, so bake in the official one.
# Pinned by release tag AND sha256: the build fails if upstream ever changes the
# artefact under the same URL.
FROM metabase/metabase:v0.63.18.4

ADD --checksum=sha256:3cba937ff2e9f39ba141460e1f4fe4a37b8c03820a0a9865cb6642fa70e9c05e \
    https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/1.53.4/clickhouse.metabase-driver.jar \
    /plugins/clickhouse.metabase-driver.jar
