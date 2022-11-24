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
                EnableIdempotence = true,
                Acks = Acks.All, // Para garantir a idempotência (sem duplicatas)
                MaxInFlight = 1, // Determina quantidade de conexões com o kafka
                MessageSendMaxRetries = 2, // Tenta enviar novamente no máximo 2 vezes
                TransactionalId = Guid.NewGuid().ToString(),
                BootstrapServers = _bootstrapServers
            };

            var payload = JsonSerializer.Serialize(message);

            var producer = new ProducerBuilder<string, T>(config)
                .SetValueSerializer(new CustomSerializer<T>())
                .Build();

            // Irá se comunicar com o coordenador de transações
            // através do Transaction Id definido acima
            // e vou começar um processo de transação, ou seja,
            // irei enviar vários eventos mas só no final
            // vou confirmar a transação se tudo ocorrer bem
            // dai você pode enviar todas as mensagens
            producer.InitTransactions(TimeSpan.FromSeconds(5));

            producer.BeginTransaction();

            // Envio mensagem 1, envio mensagem 2, etc...

            // Aborto se deu algo errado
            //producer.AbortTransaction();

            var result = await producer.ProduceAsync(topic, new Message<string, T>
            {
                Key = Guid.NewGuid().ToString(),
                Value = message
            });

            // Commito se deu tudo certo
            producer.CommitTransaction();

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
                    AutoOffsetReset = AutoOffsetReset.Latest, // Consuma somente as mensagens a partir do momento do Consume
                    //AutoOffsetReset = AutoOffsetReset.Earliest, // Consuma todas as mensagens represadas que estão no tópico
                    EnableAutoCommit = false, // false == A aplicação que se encarregará de dizer que "LEU" a mensagem do kafka
                    EnablePartitionEof = true, // true == O kafka avisa através da "" se a partição chegou ao fim
                    EnableAutoOffsetStore = false, // false == O offset da mensagem lida não será deslocado automaticamente,
                                                   // cabendo ao consumidor fazer isso manualmente
                    IsolationLevel = IsolationLevel.ReadCommitted // Para ler somente mensagens que foram confirmadas pela transação
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

                    // Utilizar se caso necessitar reprocessar a mensagem
                    // consumer.Seek(result.TopicPartitionOffset);
                    // continue;

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
