using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Messaging;
using System.Text;
using System.Threading.Tasks;

namespace OSIsoftPI2AzureEventHub
{
    class DataCompression
    {
        public byte[] CompressMessage(string toCompress, bool IsComp)
        {
            var bytes = Encoding.UTF8.GetBytes(toCompress);
            var a = new Message(Encoding.UTF8.GetBytes(toCompress));
            byte[] returnBytes = null;
            if (IsComp)
                returnBytes = CompressBytes(bytes);
            else
                returnBytes = bytes;

            return returnBytes;
        }

        byte[] CompressBytes(byte[] bytes)
        {
            var memoryStream = new MemoryStream();
            var gzipStream = new GZipStream(memoryStream, CompressionLevel.Optimal);
            gzipStream.Write(bytes, 0, bytes.Length);
            gzipStream.Close();
            return memoryStream.ToArray();
        }
    }
}
