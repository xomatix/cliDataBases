﻿using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using System.Xml.Linq;
using System.Threading;

namespace cli
{
    public class Database
    {
        private const string _connectionStringSqlite = "Data Source=test_database.db";
        private static ConnectionMultiplexer _redis = ConnectionMultiplexer.Connect("localhost");
        private static IDatabase _redisDb = _redis.GetDatabase();

        public static int CountChildrenOfAGivenNode(string nodeName)
        {
            var query = $"SELECT nn_child_count from n_node nn WHERE nn_name = '{nodeName}' limit 1";
            List<NodeModel> nodes = new List<NodeModel>();
            using (var connection = new SqliteConnection(_connectionStringSqlite))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = query;
                Console.WriteLine(command.CommandText);

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        return reader.GetInt32(0);
                    }
                }
            }
            return 0;
        }

        public static List<NodeModel> GetChildrenOfANode(string nodeName)
        {
            List<NodeModel> nodes = new List<NodeModel>();
            using (var connection = new SqliteConnection(_connectionStringSqlite))
            {
                connection.Open();
                var command = connection.CreateCommand();

                //var startTime = DateTime.Now;
                var parentNode = GetRedisValue(nodeName);
                //var endTime = DateTime.Now;

                //var elapsedTime = endTime - startTime;
                //double elapsedSeconds = elapsedTime.TotalSeconds;

                //Console.WriteLine($"Redis Elapsed Time: {elapsedSeconds:F4} seconds , in elapsed unit : {elapsedTime}");



                if (parentNode.childrenCount == 0)
                {
                    return nodes;
                }
                if (parentNode.childrenCount < 100)
                {

                    foreach (var nodeId in parentNode.childrenIds)
                    {
                        nodes.Add(GetRedisValue(nodeId));
                    }
                    return nodes;
                }

                command.CommandText = $"SELECT * from n_node nn " +
                    $"join n_relation nr on (nr.nn_children_id=nn.nn_id) " +
                    $"WHERE nr.nn_parent_id in ({parentNode.ID}) " +
                    (parentNode.childrenCount < 100 ? $"and nr.nn_children_id in ({parentNode.childrenIdsString}) " : "") +
                    $"limit ({parentNode.childrenCount});";
                //Console.WriteLine(command.CommandText);

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var id = reader.GetInt32(0);
                        var name = reader.GetString(1);
                        //var childCouint = reader.GetInt32(2);
                        nodes.Add(new NodeModel() { ID = id, Name = name });
                    }
                }
            }
            return nodes;
        }

        public static void InitSqliteDatabase()
        {
            string databaseFilePath = new SqliteConnectionStringBuilder(_connectionStringSqlite).DataSource;

            // Close any open connections (if applicable)

            // Delete the file
            if (File.Exists(databaseFilePath))
            {
                try
                {
                    File.Delete(databaseFilePath);
                }
                catch (Exception) { }
            }

            using (var connection = new SqliteConnection(_connectionStringSqlite))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = $"CREATE table if not exists  n_node (" +
                    $"nn_id SERIAL PRIMARY KEY," +
                    $"nn_name varchar(255)," +
                    $"nn_child_count INTEGER" +
                    $");" +
                    $"CREATE table if not exists  n_relation (" +
                    $"nn_parent_id INTEGER REFERENCES n_node(nn_id)," +
                    $"nn_children_id INTEGER REFERENCES n_node(nn_id)" +
                    $");";
                command.ExecuteNonQuery();

                command.CommandText = $"BEGIN;" +
                    $"DELETE from n_node;" +
                    $"DELETE from n_relation;" +
                    $"commit;";
                command.ExecuteNonQuery();
                connection.Close();
            }
        }

        public static async void InsertNodeModel(NodeModel[] nodeModels)
        {
            List<string> insertNodeQueries = new List<string>();
            List<string> insertRelationQueries = new List<string>();
            var keyValueRedisDictionary = new Dictionary<RedisKey, RedisValue>();

            int i = 0;
            int lastID = nodeModels.Last().ID;
            List<string> oneInsertQuery = new List<string>();
            List<string> oneInsertRelationQuery = new List<string>();
            foreach (NodeModel nodeModel in nodeModels)
            {
                var childrenCount = nodeModel.childrenIds.Count();
                oneInsertQuery.Add($"({nodeModel.ID},'{nodeModel.Name}',{childrenCount})");

                int currentNodeModelID = nodeModel.ID;
                nodeModel.childrenIds.Sort();
                foreach (var childrenId in nodeModel.childrenIds)
                {
                    oneInsertRelationQuery.Add($"({currentNodeModelID},{childrenId})");
                }

                keyValueRedisDictionary.Add(nodeModel.Name, $"{currentNodeModelID};{childrenCount};{string.Join(",", nodeModel.childrenIds)}");
                keyValueRedisDictionary.Add(nodeModel.ID.ToString(), $"{nodeModel.Name};{childrenCount};{string.Join(",", nodeModel.childrenIds)}");

                if (i == 10000 || lastID == nodeModel.ID)
                {
                    i = 0;
                    insertNodeQueries.Add("INSERT into n_node (nn_id,nn_name,nn_child_count) values " + string.Join(',', oneInsertQuery) + ";");
                    oneInsertQuery.Clear();

                    if (oneInsertRelationQuery.Count > 0)
                    {

                        insertRelationQueries.Add("INSERT into n_relation (nn_parent_id,nn_children_id) values " + string.Join(',', oneInsertRelationQuery) + ";");
                        oneInsertRelationQuery.Clear();
                    }
                }
                i++;
            }




            var tasks = new List<Task>();

            foreach (var insertQuery in insertNodeQueries)
            {
                tasks.Add(ExecuteInsertQueryAsync(insertQuery));
            }
            await Task.WhenAll(tasks);
            tasks.Clear();
            Console.WriteLine("Nodes insertion completed");

            foreach (var insertQuery in insertRelationQueries)
            {
                tasks.Add(ExecuteInsertQueryAsync(insertQuery));
            }

            await Task.WhenAll(tasks);
            Console.WriteLine("Relations insertion completed");

            foreach (var dict in keyValueRedisDictionary)
            {
                tasks.Add(SetRedisValue(dict.Key, dict.Value));
            }

            await Task.WhenAll(tasks);
            Console.WriteLine("Nodes fast storage insertion completed");

        }

        private async static Task SetRedisValue(string key, string value)
        {
            _redisDb.StringSet(key, value);
        }
        private static NodeModel GetRedisValue(int key)
        {
            try
            {
                var parts = _redisDb.StringGet(key.ToString()).ToString().Split(";");
                if (parts.Length == 4)
                {
                    parts[0] = parts[0] + ";" + parts[1];
                    parts[1] = parts[2];
                    parts[2] = parts[3];
                }
                if (parts.Length == 0) return null;
                string name = parts[0];
                int childrenCount = int.Parse(parts[1]);
                string childrenIdsString = parts[2];
                List<int> childrenIds = null;
                if (childrenCount != 0)
                {
                    childrenIds = parts[2].Split(",").Select(x => int.Parse(x)).ToList();
                }
                return new NodeModel() { ID = key, Name = name, childrenCount = childrenCount, childrenIdsString = childrenIdsString, childrenIds = childrenIds };
            }
            catch (Exception)
            {
                var parts = _redisDb.StringGet(key.ToString()).ToString().Split(";");
                throw new Exception(key.ToString());
            }

        }
        private static NodeModel GetRedisValue(string key)
        {
            var parts = _redisDb.StringGet(key).ToString().Split(";");
            if (parts.Length == 4)
            {
                parts[0] = parts[0] + ";" + parts[1];
                parts[1] = parts[2];
                parts[2] = parts[3];
            }
            if (parts.Length == 0) return null;
            int id = int.Parse(parts[0]);
            int childrenCount = int.Parse(parts[1]);
            string childrenIdsString = parts[2];
            List<int> childrenIds = parts[2].Split(",").Select(x => int.Parse(x)).ToList();
            return new NodeModel() { ID = id, childrenCount = childrenCount, childrenIdsString = childrenIdsString, childrenIds = childrenIds };
        }
        private static void RenameRedisKey(string oldKey, string newKey)
        {
            _redisDb.KeyRename(oldKey, newKey);
        }

        static async Task ExecuteInsertQueryAsync(string insertQuery)
        {

            using (var connection = new SqliteConnection(_connectionStringSqlite))
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    try
                    {
                        command.CommandText = insertQuery;
                        await command.ExecuteNonQueryAsync();
                    }
                    catch (Exception)
                    {
                        throw new Exception(insertQuery);
                    }

                }
            }

        }

        //public static NodeModel ReadNodeModel()
        //{
        //    NodeModel nodeModel = new NodeModel();
        //    nodeModel.ID = reader.GetInt32(reader.GetOrdinal("id"));
        //    nodeModel.Name = reader.GetString(reader.GetOrdinal("name"));
        //    nodeModel.childrenCount = reader.GetInt32(reader.GetOrdinal("nn_child_count"));
        //    return nodeModel;
        //}
    }
}
