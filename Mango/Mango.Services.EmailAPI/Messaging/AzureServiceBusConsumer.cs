using Azure.Messaging.ServiceBus;
using Mango.Services.EmailAPI.Models.Dto;
using Mango.Services.EmailAPI.Services;
using Newtonsoft.Json;
using System.Text;

namespace Mango.Services.EmailAPI.Messaging
{
    public class AzureServiceBusConsumer: IAzureServiceBusConsumer
    {
        private readonly string serviceBusConnectionString;
        private readonly string emailCartQueue;
        private readonly string registerUSerQueue;
        private readonly IConfiguration _configuration;
        private readonly EmailService _emailService;

        private ServiceBusProcessor _processorEmailCart;
        private ServiceBusProcessor _processorEmailRegister;



        public AzureServiceBusConsumer(IConfiguration configuration, EmailService emailService)
        {
            _emailService = emailService;
            _configuration = configuration;
            serviceBusConnectionString = _configuration.GetValue<string>("ServiceBusConnectionString");
            emailCartQueue = _configuration.GetValue<string>("TopicAndQueueNames:EmailShoppingCartQueue");
            registerUSerQueue = _configuration.GetValue<string>("TopicAndQueueNames:RegisterUserQueue");
            
            var client = new ServiceBusClient(serviceBusConnectionString);
            _processorEmailCart = client.CreateProcessor(emailCartQueue);
            _processorEmailRegister = client.CreateProcessor(registerUSerQueue);
        }

        public async Task Start()
        {
            _processorEmailCart.ProcessMessageAsync += OnEmailCartRequestREceivied;
            _processorEmailCart.ProcessErrorAsync += ErrorHandler;
            await _processorEmailCart.StartProcessingAsync();


            _processorEmailRegister.ProcessMessageAsync += OnUserRegisterRequestREceivied;
            _processorEmailRegister.ProcessErrorAsync += ErrorHandler;
            await _processorEmailRegister.StartProcessingAsync();
        }

        

        private Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }

        public async Task Stop()
        {
            await _processorEmailCart.StopProcessingAsync();
            await _processorEmailCart.DisposeAsync();

            await _processorEmailRegister.StopProcessingAsync();
            await _processorEmailRegister.DisposeAsync();
        }

        private async Task OnEmailCartRequestREceivied(ProcessMessageEventArgs args)
        {
            // This is where you will receive message
            var message = args.Message;
            var body = Encoding.UTF8.GetString(message.Body);

            CartDto objMessage = JsonConvert.DeserializeObject<CartDto>(body);
            try
            {
                // Try to log email
                await _emailService.EmailCartAndLog(objMessage);
                await args.CompleteMessageAsync(args.Message);

            }catch (Exception ex)
            {
                throw;
            }
        }

        private async Task OnUserRegisterRequestREceivied(ProcessMessageEventArgs args)
        {
            // This is where you will receive message
            var message = args.Message;
            var body = Encoding.UTF8.GetString(message.Body);

            string email = JsonConvert.DeserializeObject<string>(body);
            try
            {
                // Try to log email
                await _emailService.RegisterUserEmailAndLog(email);
                await args.CompleteMessageAsync(args.Message);

            }
            catch (Exception ex)
            {
                throw;
            }
        }


    }
}
