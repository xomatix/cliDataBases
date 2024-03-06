using Neo4j.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace cli
{
    public class Neo4jDatabase : IDisposable
    {
        private readonly IDriver _driver;
        private readonly string _uri = "bolt://localhost:7687";
        private readonly string _user = "neo4j";
        private readonly string _password = "12345678";

        public Neo4jDatabase()
        {
            _driver = GraphDatabase.Driver(_uri, AuthTokens.Basic(_user, _password));
        }

        public async void InsertNodesToDatabase(NodeModel[] nodeModels)
        {//2031337
            Console.WriteLine("Starting insertion to neo4j");

            //insertion query for nodes/category
            List<string> insertNodesQuery = new List<string>();
            List<string> partialNodesQuery = new List<string>();

            List<string> insertRelationsQuery = new List<string>();
            int nodesCount = nodeModels.Length;

            var threads = new List<Thread>();
            for (int i = 0; i < nodesCount; i++)
            {
                var node = nodeModels.ElementAt(i);

                node.childrenCount = node.childrenIds.Count();
                if (node.childrenCount > 0)
                    insertRelationsQuery.Add($"MATCH (a:category) WHERE a.id = {node.ID} " +
                    $"MATCH (b:category) WHERE b.id IN [{string.Join(",", node.childrenIds)}] " +
                    $"CREATE (a)-[:subcategory]->(b), " +
                    $"(b)-[:parentcategory]->(a);");

                partialNodesQuery.Add($"(:category {{id: {node.ID}, name: \"{node.Name}\", child_count: {node.childrenCount}}})");
                if ((i > 0 && i % 100 == 0) || (i + 1) == nodesCount)
                {
                    var q = $"CREATE {string.Join(",", partialNodesQuery)};";
                    //var thread = new Thread(() => ExecuteCypherQuery(q));
                    //threads.Add(thread);
                    //thread.Start();
                    insertNodesQuery.Add(q);
                    partialNodesQuery.Clear();
                }
            }

            //foreach (var insertQuery in insertNodesQuery)
            //{
            //    var thread = new Thread(() => ExecuteCypherQuery(insertQuery));
            //    threads.Add(thread);
            //    thread.Start();
            //}
            foreach (var thread in threads)
            {
                thread.Join();
            }

            var tasks = new List<Task>();
            //foreach (var query in insertNodesQuery)
            //{
            //    tasks.Add(ExecuteCypherQuery(query));
            //}

            //await Task.WhenAll(tasks);
            //Console.WriteLine("Nodes insertion completed");
            //tasks.Clear();

            foreach (var query in insertRelationsQuery)
            {
                tasks.Add(ExecuteCypherQuery(query));
            }

            await Task.WhenAll(tasks);

            //Console.WriteLine("Relations insertion completed");
        }

        public async void InitNeo4jDatabase()
        {
            await using var session = _driver.AsyncSession();
            var queryList = new string[] { "MATCH (n:category)-[r:parentcategory]->() delete r;", "MATCH (n:category)-[r:subcategory]->() delete r;", "MATCH (n:category) delete n;" };
            var greeting = await session.ExecuteWriteAsync(async tx =>
            {
                foreach (var q in queryList)
                {
                    await tx.RunAsync(q);
                }

                return true;
            });
        }

        public async Task ExecuteCypherQuery(string query)
        {
            try
            {
                using (var session = _driver.AsyncSession())
                {
                    var result = await session.RunAsync(query);

                }
            }
            catch (Exception)
            {

                throw new Exception(query);
            }
        }

        public void Dispose()
        {
            _driver?.Dispose();
        }
    }
}
