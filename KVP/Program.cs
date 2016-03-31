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
            var options = StorageEnvironmentOptions.ForPath("test");
            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "Votes");
                    env.CreateTree(tx, "CityVotes");

                    tx.Commit();
                }
                VoteFor(env, 1304, "Black", "Sao Paulo");
                VoteFor(env, 34, "Black", "Sao Paulo");
                VoteFor(env, 304, "Black", "New York");

                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var tree = tx.ReadTree("Votes");
                    using (var it = tree.Iterate())
                    {
                        if (it.Seek(Slice.BeforeAllKeys))
                        {
                            do
                            {
                                var id = it.CurrentKey.CreateReader().ReadAllAsString();
                                var value = it.CreateReaderForCurrent().ReadAllAsString();
                                Console.WriteLine(id+","+value);
                            } while (it.MoveNext());
                        }
                    }
                    Console.WriteLine();
                    Console.WriteLine("Votes in SP:");
                    var cityVotes = tx.ReadTree("CityVotes");
                    using (var it = cityVotes.MultiRead("Sao Paulo"))
                    {
                        if (it.Seek(Slice.BeforeAllKeys))
                        {
                            do
                            {
                                Console.WriteLine(it.CurrentKey.CreateReader().ReadAllAsString());
                            } while (it.MoveNext());
                        }
                    }
                }


            }
        }

        private static void VoteFor(StorageEnvironment env, int id, string color, string city)
        {
            using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = tx.ReadTree("Votes");

                if (tree.Read(id.ToString()) != null)
                    return;

                tree.Add(id.ToString(),
                    Encoding.UTF8.GetBytes(color + "," + city));

                var cityVotes = tx.ReadTree("CityVotes");
                // var cityVotes = new Dictionary<string, HashSet<string>>();
                cityVotes.MultiAdd(city, id.ToString());

                tx.Commit();
            }
        }
    }
}
