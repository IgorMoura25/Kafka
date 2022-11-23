using Confluent.Kafka;

// Configurações iniciais do Consumer
var config = new ConsumerConfig
{
    // Grupo de consumidores
    GroupId = "consumers1",

    // BootstrapServers > A URL de um ou mais brokers/servers como ponto de partida
    // em que fornecemos para que ocorra um fetch/discover inicial de metadata 
    // como tópicos, particões, etc sobre o cluster kafka
    // !!! é bom fornecer mais do que um no caso deste estar indisponível !!!
    BootstrapServers = "localhost:9092"
};

// Informando qual o tipo de dado da mensagem que será recebido (chave <string> / valor <string>)
using var consumer = new ConsumerBuilder<string, string>(config).Build();

consumer.Subscribe("topic-test");

while (true)
{
    var result = consumer.Consume();
    Console.Write($"Mensagem: {result.Message.Key}-{result.Message.Value}");
}