using System;

namespace Core.Models
{
    [Serializable]
    public class MQMessage
    {
        public string F1 { get; set; }
        public string F2 { get; set; }

        public MQMessage(int i)
        {
            F1 = i.ToString();
            F2 = i.ToString();
        }
    }
}