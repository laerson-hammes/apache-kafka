# Data Generation
FROM confluentinc/cp-kafka-connect-base:6.1.4

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components,/data/connect-jars"

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:0.4.0
RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:latest
