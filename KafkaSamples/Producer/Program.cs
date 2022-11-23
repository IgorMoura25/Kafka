using Confluent.Kafka;
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

// Configurações iniciais do Produtor
var config = new ProducerConfig
{
    // BootstrapServers > A URL de um ou mais brokers/servers como ponto de partida
    // em que fornecemos para que ocorra um fetch/discover inicial de metadata 
    // como tópicos, particões, etc sobre o cluster kafka
    // !!! é bom fornecer mais do que um no caso deste estar indisponível !!!
    BootstrapServers = "localhost:9092"
};

// Informando qual o tipo de dado da mensagem que será produzido (chave <string> / valor <Curso>)
using var producer = new ProducerBuilder<string, Curso>(config)
    .SetValueSerializer(new AvroSerializer<Curso>(schemaRegistry))
    .Build();

var message = new Message<string, Curso>
{
    //<string>
    Key = Guid.NewGuid().ToString(),
    //<Curso>
    Value = new Curso()
    {
        Id = new Guid().ToString(),
        Descricao = "Curso de Apache Kafka"
    }
};

var result = await producer.ProduceAsync("cursos", message);

Console.Write($"Partition: {result.Partition} Offset: {result.Offset}");
Console.ReadLine();
