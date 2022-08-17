using System;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;

namespace KafkaProducer
{
    internal record TimeStampRecord
    {
        public Guid EventId { get; set; }
        public Guid ParentEvent { get; set; }
        public byte[] HashValue => calcHash();

        private byte[] calcHash()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(EventId.ToString());
            sb.Append(ParentEvent.ToString());
            sb.Append(TimeStamp.ToString());

            SHA256 sHA256 = SHA256.Create();
            byte[] bytes = new UnicodeEncoding().GetBytes(sb.ToString());
            return sHA256.ComputeHash(bytes);
        }

        public DateTimeOffset TimeStamp { get; set; }
    }
}
