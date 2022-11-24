using Confluent.Kafka;
using RC.MessageBus.Kafka;
using System.Text.Json;

namespace MessageBus
{
    public class KafkaMessageBus : IKafkaMessageBus
    {
        private readonly string _bootstrapServers;

        public KafkaMessageBus(string bootstrapServers)
        {
            _bootstrapServers = bootstrapServers;
        }

        public async Task ProduceAsync<T>(string topic, T message)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _bootstrapServers
            };

            var payload = JsonSerializer.Serialize(message);

            var producer = new ProducerBuilder<string, T>(config)
                .SetValueSerializer(new CustomSerializer<T>())
                .Build();

            var result = await producer.ProduceAsync(topic, new Message<string, T>
            {
                Key = Guid.NewGuid().ToString(),
                Value = message
            });

            await Task.CompletedTask;
        }

        // executeAfterConsumed: Assim que ele consumir a mensagem, qual função irá executar?
        public async Task ConsumeAsync<T>(string topic, Func<T?, Task> executeAfterConsumed, CancellationToken cancellation)
        {
            var teste = Task.Factory.StartNew(async () =>
            {
                var config = new ConsumerConfig
                {
                    GroupId = "test-group",
                    BootstrapServers = _bootstrapServers,
                    EnableAutoCommit = false, // false == A aplicação que se encarregará de dizer que "LEU" a mensagem do kafka
                    EnablePartitionEof = true // true == O kafka avisa através da "" se a partição chegou ao fim
                };

                using var consumer = new ConsumerBuilder<string, T>(config)
                .SetValueDeserializer(new CustomDeserializer<T>())
                .Build();

                consumer.Subscribe(topic);

                while (!cancellation.IsCancellationRequested)
                {
                    var result = consumer.Consume();

                    // Se a partição chegou ao fim, consome novamente
                    if (result.IsPartitionEOF)
                    {
                        continue;
                    }

                    // Se não, quer dizer que foi consumida uma mensagem de fato, então...

                    // Executo a função que está aguardando a mensagem como parâmetro
                    await executeAfterConsumed(result.Message.Value);

                    // Digo ao kafka que a mensagem foi "LIDA"
                    consumer.Commit();
                }
            }, cancellation, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            await Task.CompletedTask;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
