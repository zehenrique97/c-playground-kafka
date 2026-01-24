#include <stdio.h>
#include <pthread.h>
#include <librdkafka/rdkafka.h>
#include <signal.h>
#include <stdatomic.h>
#include <string.h>
#include <errno.h>

static volatile sig_atomic_t running = 1;

static void stop(int sig) {
    (void) sig;
    running = 0;
}

typedef struct ProducerArgs {
    rd_kafka_t *producer;
    const char *topic;
} ProducerArgs;

typedef struct ConsumerArgs {
    rd_kafka_t *consumer;
} ConsumerArgs;

rd_kafka_t *create_producer(const char *brokers) {
    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    char errstr[512];

    if(rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Erro ao criar config do producer: %s\n", errstr);
        return NULL;
    }

    rd_kafka_t *producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));

    if(!producer) fprintf(stderr, "Erro ao criar producer: %s\n", errstr);

    return producer;
}

rd_kafka_t *create_consumer(const char *brokers, const char *group_id, char **topics, int topic_count, const char *offset_reset) {
    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    char errstr[512];

    if(
        rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "group.id", group_id, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "auto.offset.reset", offset_reset, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK
    ) {
        fprintf(stderr, "Erro ao setar config do consumer: %s\n", errstr);
        return NULL;
    }

    rd_kafka_t *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));

    if(!consumer) {
        fprintf(stderr, "Erro ao criar consumer: %s\n", errstr);
        return NULL;
    }

    rd_kafka_poll_set_consumer(consumer);

    rd_kafka_topic_partition_list_t *topic_list = rd_kafka_topic_partition_list_new(1);

    rd_kafka_topic_partition_list_add(topic_list, "playground-c", RD_KAFKA_PARTITION_UA);
    
    rd_kafka_subscribe(consumer, topic_list);
    rd_kafka_topic_partition_list_destroy(topic_list);

    return consumer;
}

void *producer_thread(void *arg) {
    ProducerArgs *producer_args = (ProducerArgs *) arg;

    char msg[100];

    while(running) {
        printf("Digite sua mensagem: ");
        
        fgets(msg, sizeof(msg), stdin);

        msg[strcspn(msg, "\n")] = '\0';

        if(strlen(msg) == 0) continue;

        rd_kafka_producev(
            producer_args->producer,
            RD_KAFKA_V_TOPIC(producer_args->topic),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE((void *) msg, strlen(msg)),
            RD_KAFKA_V_END
        );  
        rd_kafka_poll(producer_args->producer, 0);
    }

    printf("Finalizando producer\n");
    rd_kafka_flush(producer_args->producer, 5000);
    rd_kafka_destroy(producer_args->producer);

    return NULL;
}

void *consumer_thread(void *arg) {
    ConsumerArgs *consumer_args = (ConsumerArgs *) arg;

    while(running) {
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(consumer_args->consumer, 1000);

        if (!msg) continue;

        if(msg->err) {
            printf("Erro ao ler o tópico: %s\n", rd_kafka_message_errstr(msg));
            continue;
        }

        printf("Mensagem recebida: %.*s\n", (int)msg->len, (const char *)msg->payload);
        rd_kafka_message_destroy(msg);
    }

    printf("Finalizando consumer\n");
    rd_kafka_consumer_close(consumer_args->consumer);
    rd_kafka_destroy(consumer_args->consumer);

    return NULL;
}

int main() {
    signal(SIGINT, stop);

    const char *brokers = "localhost:9092";
    const char *group_id = "test_group";
    const char *offset_reset = "latest";
    char topics[] = {"playground-c"};

    rd_kafka_t *producer = create_producer(brokers);
    ProducerArgs p_args = {.producer = producer, .topic = "playground-c"};
    pthread_t p_thread;
    pthread_create(&p_thread, NULL, producer_thread, &p_args);

    rd_kafka_t *consumer = create_consumer(brokers, group_id, topics, 1, offset_reset);
    ConsumerArgs c_args = {.consumer = consumer};
    pthread_t c_thread;
    pthread_create(&c_thread, NULL, consumer_thread, &c_args);

    pthread_join(p_thread, NULL);
    pthread_join(c_thread, NULL);

    return 0;
}