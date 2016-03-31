using Voron;

namespace KVP
{
    class Program
    {
        static void Main(string[] args)
        {
            var options = StorageEnvironmentOptions.CreateMemoryOnly();
            using (var env = new StorageEnvironment(options))
            {
                


            }
        }
    }
}
