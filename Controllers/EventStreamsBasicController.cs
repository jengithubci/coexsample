using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

using Confluent.Kafka;

namespace SampleAPI.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class EventStreamsBasicController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<EventStreamsBasicController> _logger;

        private readonly IProducer<string, string> _producer;
        private readonly IConsumer<string, string> _consumer;

        public EventStreamsBasicController(ILogger<EventStreamsBasicController> logger)
        {
            _logger = logger;

            var pconfig = new ProducerConfig()
            {
                BootstrapServers = "broker-2-h9vp4hl91n22n1vx.kafka.svc09.us-south.eventstreams.cloud.ibm.com:9093,broker-3-h9vp4hl91n22n1vx.kafka.svc09.us-south.eventstreams.cloud.ibm.com:9093,broker-5-h9vp4hl91n22n1vx.kafka.svc09.us-south.eventstreams.cloud.ibm.com:9093,broker-0-h9vp4hl91n22n1vx.kafka.svc09.us-south.eventstreams.cloud.ibm.com:9093,broker-4-h9vp4hl91n22n1vx.kafka.svc09.us-south.eventstreams.cloud.ibm.com:9093,broker-1-h9vp4hl91n22n1vx.kafka.svc09.us-south.eventstreams.cloud.ibm.com:9093",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = "token",
                SaslPassword = "dZGfeKQkx5d9siHlKdcIOzOWADyp_GD2wyc7Z4q-jMOm",
                ClientId = "kafka-java-console-sample-producer",
                Partitioner=((int)new Partitioner())
            };
            _producer = new ProducerBuilder<string, string>(pconfig).Build();


             var cconfig = new ConsumerConfig()
            {
                BootstrapServers = "broker-2-h9vp4hl91n22n1vx.kafka.svc09.us-south.eventstreams.cloud.ibm.com:9093,broker-3-h9vp4hl91n22n1vx.kafka.svc09.us-south.eventstreams.cloud.ibm.com:9093,broker-5-h9vp4hl91n22n1vx.kafka.svc09.us-south.eventstreams.cloud.ibm.com:9093,broker-0-h9vp4hl91n22n1vx.kafka.svc09.us-south.eventstreams.cloud.ibm.com:9093,broker-4-h9vp4hl91n22n1vx.kafka.svc09.us-south.eventstreams.cloud.ibm.com:9093,broker-1-h9vp4hl91n22n1vx.kafka.svc09.us-south.eventstreams.cloud.ibm.com:9093",
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslUsername = "token",
                SaslPassword = "dZGfeKQkx5d9siHlKdcIOzOWADyp_GD2wyc7Z4q-jMOm",
                GroupId = "kafka-java-console-sample-consumer",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            _consumer = new ConsumerBuilder<string, string>(cconfig).Build();

        }

        [HttpGet]
        public IEnumerable<WeatherForecast> Get()
        {
            var rng = new Random();
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateTime.Now.AddDays(index),
                TemperatureC = rng.Next(-20, 55),
                Summary = Summaries[rng.Next(Summaries.Length)]
            })
            .ToArray();
        }

        [HttpPut("{message}")]
        public string PutMessage( string message)
        {
            return "Success - message";
        }

        [HttpPost]
        public async Task<string> PostMessage( string message)
        {
            
            var dr = await _producer.ProduceAsync("kafka-java-console-sample-topic", new Message<string, string>()
            {
                Key = "key",
                Value = "message"
            });

            _consumer.Subscribe("kafka-java-console-sample-topic");
            var consumeResult =_consumer.Consume();
            var result =  "Key - " + consumeResult.Message.Key.ToString() + " | " + "Message - " + consumeResult.Message.Value.ToString();

            return result;
        }
    }
}
