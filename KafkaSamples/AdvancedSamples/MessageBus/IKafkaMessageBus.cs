namespace MessageBus
{
    public interface IKafkaMessageBus : IDisposable
    {
        Task ProduceAsync<T>(string topic, T message);

        // executeAfterConsumed: Assim que ele consumir a mensagem, qual função irá executar?
        Task ConsumeAsync<T>(string topic, Func<T?, Task> executeAfterConsumed, CancellationToken cancellation);
    }
}