using Core.Models;
using Core.Utils;
using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;

namespace PushMQ
{
    public class Process
    {
        private bool fagRun;
        public bool IsRunning { get { return ThreadRunning > 0; } }
        public int MaxThreads { get; private set; }
        public string MQHostName { get; private set; }
        public string MQUserName { get; private set; }
        public string MQPassword { get; private set; }
        public string MQQueueName { get; private set; }
        private readonly object locker = new object();
        private int _threadRunning;

        private int ThreadRunning
        {
            get { lock (locker) { return _threadRunning; } }
            set { lock (locker) { _threadRunning = value; } }
        }

        public Process()
        {
            MaxThreads = Convert.ToInt32(ConfigurationManager.AppSettings["MaxThreads"]);
            MQHostName = ConfigurationManager.AppSettings["MQHostName"];
            MQUserName = ConfigurationManager.AppSettings["MQUserName"];
            MQPassword = ConfigurationManager.AppSettings["MQPassword"];
            MQQueueName = ConfigurationManager.AppSettings["MQQueueName"];
        }

        public void Start()
        {
            fagRun = true;
            Run();
        }

        private void Run()
        {
            Task.Factory.StartNew(() => ProcessData());
        }

        public void Stop()
        {
            fagRun = false;
        }

        public void ProcessData()
        {
            PushMQ(1000);
        }

        private bool PushMQ(int numberMsg)
        {
            int i = 1;
            while (i < numberMsg)
            {
                int count = 0;
                var message = new MQMessage(i);
                while (!MQUtils.Push(message))
                {
                    Console.WriteLine("{0} Có lỗi khi gửi lên queue. Thử lại lần {1}", i, count++);
                    Thread.Sleep(3000);
                }
                Console.WriteLine(" [x] Sent {0}", i++);
            }
            Console.WriteLine("Push all msg done!");
            return true;
        }
    }
}