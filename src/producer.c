#include <stdio.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

int main() {
    const char *brokers = "localhost:9092";
    const char *topic = "playground-c";
    const char *msg = "Olá mundo";

    char errstr[512];

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr));

    rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));

    rd_kafka_producev(producer, RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_VALUE((void *)msg, strlen(msg)), RD_KAFKA_V_END);

    rd_kafka_flush(producer, 5000);

    rd_kafka_destroy(producer);

    return 0;
}