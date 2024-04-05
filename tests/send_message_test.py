from confluent_kafka import Producer

# Configurações do Kafka
bootstrap_servers = 'localhost:9092'  # ->  Define o endereço e a porta dos servidores Kafka.
producer_topic = 'planilha_topic'  # -> Define o tópico do Kafka para o qual o produtor irá enviar as mensagens (dados analisados).

# Configuração do produtor
producer = Producer({'bootstrap.servers': bootstrap_servers})  # -> Cria uma instância do Producer com as configurações do Kafka.

producer.produce(producer_topic, 'Mensagem de Teste 2'.encode('utf-8'))  # -> Envia o resultado do processamento de volta para o Kafka no tópico especificado.
producer.flush()  # -> Garante que todas as mensagens produzidas sejam enviadas antes de continuar.

print('Mensagem Enviada!')
