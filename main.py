from confluent_kafka import Consumer, Producer
import functions.excel_functions.excel_manipulation as funcoes_planilha
from io import BytesIO
import json

# Configurações do Kafka
bootstrap_servers = 'localhost:9092'  # ->  Define o endereço e a porta dos servidores Kafka.
consumer_group_id = 'my_consumer_group'  # -> Define o grupo de consumidores ao qual o consumidor pertence.
consumer_topic = 'planilha_topic'  # -> Define o tópico do Kafka a partir do qual o consumidor irá consumir as mensagens (planilhas).
producer_topic = 'dados_analisados_topic'  # -> Define o tópico do Kafka para o qual o produtor irá enviar as mensagens (dados analisados).

# Configuração do consumidor
# Cria uma instância do Consumer com as configurações definidas anteriormente.
consumer = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': consumer_group_id,
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([consumer_topic])  # -> Inscreve o consumidor no tópico especificado para começar a consumir mensagens desse tópico.

# Configuração do produtor
producer = Producer({'bootstrap.servers': bootstrap_servers})  # -> Cria uma instância do Producer com as configurações do Kafka.

# Loop de consumo e processamento da planilha
try:
    while True:  # -> Um loop é iniciado para consumir continuamente as mensagens (planilhas) do Kafka.
        msg = consumer.poll(timeout=1.0)  # -> O consumidor aguarda até 1 segundo por uma nova mensagem do Kafka.
        if msg is None:
            continue
        if msg.error():
            print("Erro ao consumir a mensagem: {}".format(msg.error()))
            continue

        with BytesIO(msg.value()) as planilha_temporaria:
            result = funcoes_planilha.pegar_dados_intervalo_planilha(planilha_temporaria, 'A1:E', True)

        producer.produce(producer_topic, json.dumps(result))  # -> Envia o resultado do processamento de volta para o Kafka no tópico especificado.
        producer.flush()  # -> Garante que todas as mensagens produzidas sejam enviadas antes de continuar.
        print('Mensagem analisada e enviada!')
except KeyboardInterrupt:
    pass
finally:
    consumer.close()  # -> Fecha o consumidor do Kafka de forma adequada, liberando os recursos associados a ele.
    # producer.close()
