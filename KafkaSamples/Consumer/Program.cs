using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Schemas;

// Configurações iniciais do Schema Registry
var schemaConfig = new SchemaRegistryConfig
{
    Url = "http://localhost:8081"
};

// O cliente irá bater no Schema Registry e irá salvar
// os schemas em um cache, para evitar de ficar buscando toda hora
var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);

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

// Informando qual o tipo de dado da mensagem que será recebido (chave <string> / valor <Curso>)
using var consumer = new ConsumerBuilder<string, Curso>(config)
    .SetValueDeserializer(new AvroDeserializer<Curso>(schemaRegistry).AsSyncOverAsync())
    .Build();

consumer.Subscribe("cursos");

while (true)
{
    var result = consumer.Consume();
    Console.Write($"Mensagem: {result.Message.Key}-{result.Message.Value.Id}, {result.Message.Value.Descricao}");
}