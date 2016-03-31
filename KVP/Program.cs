using System;
using System.Text;
using Voron;
using Voron.Trees;
using Voron.Util.Conversion;

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
                    env.CreateTree(tx, "VotesCounts");

                    tx.Commit();
                }
                VoteFor(env, 1304, "Elemar", "Black", "Sao Paulo");
                VoteFor(env, 34, "Filipe", "Blue", "Sao Paulo");
                VoteFor(env, 304, "Ariel", "Black", "New York");

                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var votes = tx.ReadTree("Votes");
                    using (var it = votes.Iterate())
                    {
                        if (it.Seek(Slice.BeforeAllKeys))
                        {
                            do
                            {
                                var id = it.CurrentKey.CreateReader().ReadBigEndianInt32();
                                var value = it.CreateReaderForCurrent().ReadAllAsString();
                                Console.WriteLine(id + "," + value);
                            } while (it.MoveNext());
                        }
                    }
                    //Console.WriteLine();

                    //var votesCounts = tx.ReadTree("VotesCounts");

                    //using (var it = votesCounts.Iterate())
                    //{
                    //    if (it.Seek(Slice.BeforeAllKeys))
                    //    {
                    //        do
                    //        {
                    //            var color = it.CurrentKey.CreateReader().ReadAllAsString();
                    //            var value = it.CreateReaderForCurrent().ReadLittleEndianInt64();

                    //            Console.WriteLine($"{color} = {value}");
                    //        } while (it.MoveNext());
                    //    }
                    //}
                    //Console.WriteLine();
                    //Console.WriteLine("Votes in SP:");


                    var cityVotes = tx.ReadTree("CityVotes");
                    Console.WriteLine(cityVotes.State.EntriesCount);
                    using (var it = cityVotes.Iterate())
                    {
                        if (it.Seek(Slice.BeforeAllKeys))
                        {
                            do
                            {
                                Console.WriteLine(
                                    it.CurrentKey.CreateReader().ReadAllAsString() + " "
                                    +
                                cityVotes.MultiCount(it.CurrentKey));
                                
                            } while (it.MoveNext());
                        }
                    }
                    //using (var it = cityVotes.MultiRead("Sao Paulo"))
                    //{
                    //    if (it.Seek(Slice.BeforeAllKeys))
                    //    {
                    //        do
                    //        {
                    //            var readResult = votes.Read(it.CurrentKey);
                    //            Console.WriteLine(readResult.Reader.ReadAllAsString());
                    //        } while (it.MoveNext());
                    //    }
                    //}
                }


            }
        }

        private static void VoteFor(StorageEnvironment env, int id, string name, string color, string city)
        {
            using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = tx.ReadTree("Votes");

                if (tree.Read(id.ToString()) != null)
                    return;

                var key = new Slice(EndianBitConverter.Big.GetBytes(id));
                tree.Add(key, 
                    Encoding.UTF8.GetBytes(name + "," + color + "," + city));

                var cityVotes = tx.ReadTree("CityVotes");
                // var cityVotes = new Dictionary<string, HashSet<string>>();
                cityVotes.MultiAdd(city, key);

                var votesCounts = tx.ReadTree("VotesCounts");

                var c = votesCounts.Increment(color, 1);

                //Console.WriteLine($"{c} votes for {color}");

                tx.Commit();
            }
        }
    }
}
