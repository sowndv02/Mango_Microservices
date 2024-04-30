using Azure.Messaging.ServiceBus;
using Mango.Services.RewardAPI.Message;
using Mango.Services.RewardAPI.Services;
using Newtonsoft.Json;
using System.Text;

namespace Mango.Services.RewardAPI.Messaging
{
    public class AzureServiceBusConsumer : IAzureServiceBusConsumer
    {
        private readonly string serviceBusConnectionString;
        private readonly string orderCreatedTopic;
        private readonly string orderCreatedRewardSubcription;
        private readonly IConfiguration _configuration;
        private readonly RewardService _rewardService;

        private ServiceBusProcessor _processorReward;

        public AzureServiceBusConsumer(IConfiguration configuration, RewardService rewardService)
        {
            _rewardService = rewardService;
            _configuration = configuration;
            serviceBusConnectionString = _configuration.GetValue<string>("ServiceBusConnectionString");
            orderCreatedTopic = _configuration.GetValue<string>("TopicAndQueueNames:OrderCreatedTopic");
            orderCreatedRewardSubcription = _configuration.GetValue<string>("TopicAndQueueNames:OrderCreated_Rewards_Subcription");

            var client = new ServiceBusClient(serviceBusConnectionString);
            _processorReward = client.CreateProcessor(orderCreatedTopic, orderCreatedRewardSubcription);
        }

        public async Task Start()
        {
            _processorReward.ProcessMessageAsync += OnNewOrderRewardsRequesReceived;
            _processorReward.ProcessErrorAsync += ErrorHandler;
            await _processorReward.StartProcessingAsync();
        }

        private Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }

        public async Task Stop()
        {
            await _processorReward.StopProcessingAsync();
            await _processorReward.DisposeAsync();
        }

        private async Task OnNewOrderRewardsRequesReceived(ProcessMessageEventArgs args)
        {
            // This is where you will receive message
            var message = args.Message;
            var body = Encoding.UTF8.GetString(message.Body);

            RewardsMessage objMessage = JsonConvert.DeserializeObject<RewardsMessage>(body);
            try
            {
                // Try to log email
                await _rewardService.UpdateRewards(objMessage);
                await args.CompleteMessageAsync(args.Message);

            }
            catch (Exception ex)
            {
                throw;
            }
        }
    }
}
