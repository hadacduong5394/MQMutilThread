using Core.Models;
using Core.Utils;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;

namespace PullMQ
{
    public class Process
    {
        private int ThreadRunning { get; set; }
        public int MaxThreads { get; private set; }
        public string MQHostName { get; private set; }
        public string MQUserName { get; private set; }
        public string MQPassword { get; private set; }
        public string MQQueueName { get; private set; }
        List<Task> tasks;
        public bool IsRunning { get { return ThreadRunning > 0; } }

        public Process()
        {
            MaxThreads = Convert.ToInt32(ConfigurationManager.AppSettings["MaxThreads"]);
            MQHostName = ConfigurationManager.AppSettings["MQHostName"];
            MQUserName = ConfigurationManager.AppSettings["MQUserName"];
            MQPassword = ConfigurationManager.AppSettings["MQPassword"];
            MQQueueName = ConfigurationManager.AppSettings["MQQueueName"];
            tasks = new List<Task>();
        }

        internal void Start()
        {
            Run();
        }

        private void Run()
        {
            //bat len n luong xu ly du lieu
            while (tasks.Count < MaxThreads)
                tasks.Add(Task.Factory.StartNew(() => RunProcess()));
        }

        private void RunProcess()
        {
            ThreadRunning++;
            PullMQ();
            ThreadRunning--;
        }

        private void PullMQ()
        {
            Console.WriteLine("Begin pull msg with new thread");

            var MQQueueName = ConfigurationManager.AppSettings["MQQueueName"];
            var factory = MQUtils.MQFactory;
            using (var conn = factory.CreateConnection())
            {
                using (RabbitMQ.Client.IModel channel = conn.CreateModel())
                {
                    IDictionary<string, object> queueArgs = new Dictionary<string, object>
                        {
                            {"x-ha-policy", "all"}
                        };
                    channel.QueueDeclare(queue: MQQueueName,
                                         durable: true,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: queueArgs);
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                    while (true)
                    {
                        if (!MQUtils.Pull(channel, ExcuteMessage))
                        {
                            Console.WriteLine("{0} - Queue is empty!", Thread.CurrentThread.ManagedThreadId);
                            Thread.Sleep(1000);
                        }
                    }
                }
            }
        }

        private bool ExcuteMessage(MQMessage msg)
        {
            if (msg != null)
            {
                Console.WriteLine(string.Format("[F1:F2] = [{0}:{1}]", msg.F1, msg.F2));
                return true;
            }
            return false;
        }
    }
}