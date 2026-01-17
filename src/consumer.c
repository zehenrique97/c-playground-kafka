#include <stdio.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

int main() {
    const char *brokers = "localhost:9092";
    const char *topic = "playground-c";
    const char *group = "group1";

    char errstr[512];

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr, sizeof(errstr));

    rd_kafka_t *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    rd_kafka_poll_set_consumer(consumer);

    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
    rd_kafka_subscribe(consumer, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    rd_kafka_message_t *msg = rd_kafka_consumer_poll(consumer, 60000);

    printf("Message: %s\n", (const char *)msg->payload);

    if(msg) rd_kafka_message_destroy(msg);

    rd_kafka_consumer_close(consumer);
    rd_kafka_destroy(consumer);

    return 0;
}