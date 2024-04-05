def create_topic():
    from confluent_kafka.admin import AdminClient, NewTopic

    # Criar um cliente Admin para administrar o Kafka
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})

    # Criar um tópico
    new_topics = [NewTopic("other_test_topic", num_partitions=1, replication_factor=1)]
    info = admin_client.create_topics(new_topics)

    # Wait for each operation to finish. (Parte essencial para a criação do tópico)
    for topic, f in info.items():
        try:
            f.result()  # The result itself is None
            # print(f'Topic: {topic} | F: {f}')
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))


def list_topics():
    from confluent_kafka.admin import AdminClient

    # Configurações do Kafka
    bootstrap_servers = 'localhost:9092'

    # Criar um cliente Admin para administrar o Kafka
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    # Listar todos os tópicos
    topics = admin_client.list_topics().topics

    # Exibir os tópicos
    for topic_name in topics:
        print(topic_name)

    # Fechar o cliente Admin
    # admin_client.close()


if __name__ == '__main__':
    # create_topic()
    list_topics()
