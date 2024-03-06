using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace cli
{
    public class CsvDataLoader
    {

        public void LoadCsvFile(string fileName)
        {
            Console.WriteLine("Loading csv file");
            Dictionary<string, NodeModel> hashMap = CreateRelationsHashMap(fileName);
            Console.WriteLine("Loaded csv file");

            //make some logic to save relations to database
            Database.InsertNodeModel(hashMap.Select(x => x.Value).ToArray());
            //new Neo4jDatabase().InsertNodesToDatabase(hashMap.Select(x => x.Value).ToArray());
        }

        private Dictionary<string, NodeModel> CreateRelationsHashMap(string fileName)
        {
            int nodesNum = 2031337;
            int lastPercentage = 0;
            int currentPercentage = 0;
            Dictionary<string, NodeModel> hashMap = new Dictionary<string, NodeModel>();
            try
            {
                var filestream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
                var file = new StreamReader(filestream);
                string? lineOfText = "";
                int i = 0;
                while ((lineOfText = file.ReadLine()) != null)
                {
                    var parentChild = ParseCsvLine(lineOfText);

                    var childrenNodeExists = hashMap.TryGetValue(parentChild.Item2, out NodeModel? childNode);
                    if (!childrenNodeExists)
                    {
                        i++;
                        hashMap.Add(parentChild.Item2, new NodeModel() { ID = i, Name = parentChild.Item2 });
                        childNode = hashMap.GetValueOrDefault(parentChild.Item2);
                    }

                    var parentNodeExists = hashMap.TryGetValue(parentChild.Item1, out NodeModel? parentNode);
                    if (!parentNodeExists)
                    {
                        i++;
#pragma warning disable CS8602 // Dereference of a possibly null reference.
                        hashMap.Add(parentChild.Item1, new NodeModel() { ID = i, Name = parentChild.Item1, childrenIds = [childNode.ID] });
#pragma warning restore CS8602 // Dereference of a possibly null reference.
                    }
                    if (parentNodeExists)
                    {
#pragma warning disable CS8602 // Dereference of a possibly null reference.
                        parentNode.childrenIds.Add(childNode.ID);
#pragma warning restore CS8602 // Dereference of a possibly null reference.
                    }

                    // track progress
                    currentPercentage = (i * 100) / nodesNum;
                    if (currentPercentage >= lastPercentage + 5)
                    {
                        lastPercentage = currentPercentage;
                        Console.WriteLine(currentPercentage + "%");
                    }

                }
            }
            catch (IOException e)
            {
                Console.WriteLine($"The file could not be read: {fileName}");
                Console.WriteLine(e.Message);
            }
            return hashMap;
        }

        private (string, string) ParseCsvLine(string csv)
        {
            string[] parts = csv.Replace("\\\"", "\"").Replace("'","''").Split("\",\"");
            string first = parts[0].Substring(1);
            string second = parts[1].Remove(parts[1].Length - 1);
            return (first, second);
        }
    }
}
