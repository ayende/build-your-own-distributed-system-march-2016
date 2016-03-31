using System;
using System.Text;
using Voron;
using Voron.Trees;

namespace KVP
{
    class Program
    {
        static void Main(string[] args)
        {
            var options = StorageEnvironmentOptions.CreateMemoryOnly();
            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "Values");

                    tx.Commit();
                }
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = tx.ReadTree("Values");
                    tree.Add("Oren", Encoding.UTF8.GetBytes("Eini"));
                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var tree = tx.ReadTree("Values");
                    var readResult = tree.Read("Oren");
                    Console.WriteLine(readResult.Reader.ReadAllAsString());

                    tx.Commit();
                }


            }
        }
    }
}
