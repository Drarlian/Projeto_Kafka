from confluent_kafka import Consumer

# Configurações do Kafka
bootstrap_servers = 'localhost:9092'  # ->  Define o endereço e a porta dos servidores Kafka.
consumer_group_id = 'my_consumer_group'  # -> Define o grupo de consumidores ao qual o consumidor pertence.
consumer_topic = 'dados_analisados_topic'  # -> Define o tópico do Kafka a partir do qual o consumidor irá consumir as mensagens (planilhas).

# Configuração do consumidor
# Cria uma instância do Consumer com as configurações definidas anteriormente.
consumer = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': consumer_group_id,
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([consumer_topic])  # -> Inscreve o consumidor no tópico especificado para começar a consumir mensagens desse tópico.

# Loop de consumo e processamento da planilha
try:
    while True:  # -> Um loop é iniciado para consumir continuamente as mensagens (planilhas) do Kafka.
        msg = consumer.poll(timeout=1.0)  # -> O consumidor aguarda até 1 segundo por uma nova mensagem do Kafka.
        if msg is None:
            continue
        if msg.error():
            print("Erro ao consumir a mensagem: {}".format(msg.error()))
            continue
        print(msg)
        print(msg.value())
except KeyboardInterrupt:
    pass
finally:
    consumer.close()  # -> Fecha o consumidor do Kafka de forma adequada, liberando os recursos associados a ele.
    # producer.close()
