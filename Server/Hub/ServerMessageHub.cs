using Apache.NMS;
using Apache.NMS.AMQP;
using Newtonsoft.Json;
using Server.Interfaces;
using Shared.Helpers;
using Shared.Models;
using Shared.Repositories;
using Shared.Requests;
using Shared.Responses;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Server.Hub
{
    public class ServerMessageHub : IServerMessageHub
    {
        private readonly string _brokerUri = "amqp://localhost:5672"; // ActiveMQ AMQP URI
        private readonly string _clientQueueName = "client-queue"; // Client's queue name
        private readonly string _serverQueueName = "server-queue"; // Server's queue name

        readonly ICompanyRepository _companyRepository;
        readonly ICarRepository _carRepository;

        public ServerMessageHub(ICompanyRepository companyRepository, ICarRepository carRepository)
        {
            _companyRepository = companyRepository;
            _carRepository = carRepository;
        }

        public async Task CheckForNewClientMessage()
        {
            while (true)
            {
                var nextPackage = await ReceiveMessageFromActiveMQ(_clientQueueName);
                if (nextPackage == null) break;

                await HandleMessageFromClient(nextPackage);
            }
        }

        public async Task HandleMessageFromClient(ClientQueueEntity queuePackage)
        {
            string[] classNameParts = queuePackage.TypeName.Split('.');
            string simpleClassName = classNameParts[^1];
            var requestMessage = JsonConvert.DeserializeObject(queuePackage.Content, Helpers.GetType(queuePackage.TypeName));

            Task<object> result = simpleClassName switch
            {
                nameof(CreateCarRequest) => HandleCreateCarRequest((CreateCarRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(CreateCompanyRequest) => HandleCreateCompanyRequest((CreateCompanyRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(DeleteCarRequest) => HandleDeleteCarRequest((DeleteCarRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(DeleteCompanyRequest) => HandleDeleteCompanyRequest((DeleteCompanyRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(GetCarRequest) => HandleGetCarRequest((GetCarRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(GetCarsRequest) => HandleGetCarsRequest((GetCarsRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(GetCompanyRequest) => HandleGetCompanyRequest((GetCompanyRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(GetCompaniesRequest) => HandleGetCompaniesRequest((GetCompaniesRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(UpdateCarRequest) => HandleUpdateCarRequest((UpdateCarRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(UpdateCompanyRequest) => HandleUpdateCompanyRequest((UpdateCompanyRequest)requestMessage).ContinueWith(task => (object)task.Result),
                _ => throw new NotSupportedException($"Request type {queuePackage.TypeName} is not supported.")
            };

            object actualResult = await result;
            await SendMessageToClient(actualResult, queuePackage.CorrelationId);
        }

        public async Task SendMessageToClient(object message, Guid correlationId)
        {
            var result = Helpers.ConvertObjectToJson(message);
            var entity = new ServerQueueEntity
            {
                CorrelationId = correlationId,
                Content = result.Item1,
                TypeName = result.Item2.ToString(),
                Created = DateTime.Now,
                StatusDate = DateTime.Now,
                QueueStatus = QueueStatus.New
            };

            // Send the response to the server queue using ActiveMQ
            await SendMessageToActiveMQ(_serverQueueName, entity);
        }

        private async Task SendMessageToActiveMQ(string queueName, ServerQueueEntity entity)
        {
            var factory = new NmsConnectionFactory(_brokerUri);
            using var connection = factory.CreateConnection();
            using var session = connection.CreateSession();
            IDestination destination = session.GetQueue(queueName);

            using (var producer = session.CreateProducer(destination))
            {
                var textMessage = session.CreateTextMessage(JsonConvert.SerializeObject(entity));
                textMessage.Properties["CorrelationId"] = entity.CorrelationId.ToString();
                producer.Send(textMessage);
            }
        }

        private async Task<ClientQueueEntity> ReceiveMessageFromActiveMQ(string queueName)
        {
            var factory = new NmsConnectionFactory(_brokerUri);
            using var connection = factory.CreateConnection();
            using var session = connection.CreateSession();
            IDestination destination = session.GetQueue(queueName);

            using (var consumer = session.CreateConsumer(destination))
            {
                connection.Start();
                var message = consumer.Receive();
                if (message is ITextMessage textMessage)
                {
                    var content = textMessage.Text;
                    var correlationId = Guid.Parse(textMessage.Properties["CorrelationId"].ToString());
                    return new ClientQueueEntity
                    {
                        CorrelationId = correlationId,
                        Content = content,
                        TypeName = textMessage.GetType().ToString()
                    };
                }
                return null;
            }
        }

        // Message Handlers
        private async Task<CreateCarResponse> HandleCreateCarRequest(CreateCarRequest request)
        {
            await _carRepository.AddCarAsync(request.Car);
            return new CreateCarResponse { DataId = request.DataId, Car = request.Car };
        }

        private async Task<CreateCompanyResponse> HandleCreateCompanyRequest(CreateCompanyRequest request)
        {
            await _companyRepository.AddCompanyAsync(request.Company);
            return new CreateCompanyResponse { DataId = request.DataId, Company = request.Company };
        }

        private async Task<DeleteCarResponse> HandleDeleteCarRequest(DeleteCarRequest request)
        {
            await _carRepository.RemoveCarAsync(request.CarId);
            return new DeleteCarResponse { DataId = request.DataId };
        }

        private async Task<DeleteCompanyResponse> HandleDeleteCompanyRequest(DeleteCompanyRequest request)
        {
            await _companyRepository.RemoveCompanyAsync(request.CompanyId);
            return new DeleteCompanyResponse { DataId = request.DataId };
        }

        private async Task<GetCarResponse> HandleGetCarRequest(GetCarRequest request)
        {
            var car = await _carRepository.GetCarAsync(request.CarId);
            return new GetCarResponse { DataId = request.DataId, Car = car };
        }

        private async Task<GetCarsResponse> HandleGetCarsRequest(GetCarsRequest request)
        {
            var cars = await _carRepository.GetAllCarsAsync();
            return new GetCarsResponse { DataId = request.DataId, Cars = cars.ToList() };
        }

        private async Task<GetCompanyResponse> HandleGetCompanyRequest(GetCompanyRequest request)
        {
            var company = await _companyRepository.GetCompanyAsync(request.CompanyId);
            return new GetCompanyResponse { DataId = request.DataId, Company = company };
        }

        private async Task<GetCompaniesResponse> HandleGetCompaniesRequest(GetCompaniesRequest request)
        {
            var companies = await _companyRepository.GetAllCompaniesAsync();
            return new GetCompaniesResponse { DataId = request.DataId, Companies = companies.ToList() };
        }

        private async Task<UpdateCarResponse> HandleUpdateCarRequest(UpdateCarRequest request)
        {
            await _carRepository.UpdateCarAsync(request.Car);
            return new UpdateCarResponse { DataId = request.DataId, Car = request.Car };
        }

        private async Task<UpdateCompanyResponse> HandleUpdateCompanyRequest(UpdateCompanyRequest request)
        {
            await _companyRepository.UpdateCompanyAsync(request.Company);
            return new UpdateCompanyResponse { DataId = request.DataId, Company = request.Company };
        }
    }
}
