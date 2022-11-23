using Confluent.Kafka;

// Configurações iniciais do Produtor
var config = new ProducerConfig
{
    // BootstrapServers > A URL de um ou mais brokers/servers como ponto de partida
    // em que fornecemos para que ocorra um fetch/discover inicial de metadata 
    // como tópicos, particões, etc sobre o cluster kafka
    // !!! é bom fornecer mais do que um no caso deste estar indisponível !!!
    BootstrapServers = "localhost:9092"
};

// Informando qual o tipo de dado da mensagem que será produzido (chave <string> / valor <string>)
using var producer = new ProducerBuilder<string, string>(config).Build();

var message = new Message<string, string>
{
    //<string>
    Key = Guid.NewGuid().ToString(),
    //<string>
    Value = $"Mensagem teste {DateTime.Now}"
};

var result = await producer.ProduceAsync("topic-test", message);

Console.Write($"Partition: {result.Partition} Offset: {result.Offset}");
Console.ReadLine();
