using System.Net;

namespace cli
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var startTime = DateTime.Now;
            //Console.WriteLine(string.Join(" ", args));
            int instructionNumber = 0;
            if (args.Length > 0)
            {
                try
                {
                    instructionNumber = int.Parse(args[0]);
                }
                catch (Exception)
                {
                    throw new Exception($"Can't parse instruction number {args[0]} to int!");
                }
            }

            string firstArgument = "";
            if (args.Length > 1)
            {
                firstArgument = args[1].Replace("'", "");
            }
            string secondArgument = "";
            if (args.Length > 2)
            {
                secondArgument = args[2].Replace("'", "");
            }

            //instructionNumber = 99;
            //firstArgument = "Albums_by_artist";
            InstructionSelector(instructionNumber, firstArgument, secondArgument);

            var endTime = DateTime.Now;

            var elapsedTime = endTime - startTime;
            double elapsedSeconds = elapsedTime.TotalSeconds;

            Console.WriteLine($"Elapsed Time: {elapsedSeconds:F4} seconds , in elapsed unit : {elapsedTime}");

        }

        private static void InstructionSelector(int instructionNumber, string firstArgument, string secondArgument)
        {
            //1. Finds all children of a given node
            if (instructionNumber == 1)
            {
                var nodes = Database.GetChildrenOfANode(firstArgument);
                Console.WriteLine(string.Join(", ", nodes.Select(x => x.Name)));
            }
            //2.counts all children of a given node
            if (instructionNumber == 2)
            {
                var nodes = Database.CountChildrenOfAGivenNode(firstArgument);
                Console.WriteLine("Child nodes count: " + nodes);
            }
            //3. fnds all grand children of a given node
            if (instructionNumber == 3)
            {
                var nodes = Database.FindGrandChildrenOfAGivenNode(firstArgument);
                Console.WriteLine(string.Join(", ", nodes.Select(x => x.Name)));
            }
            //4. Fnds all parents of a given node
            if (instructionNumber == 4)
            {
                var nodes = Database.FindParentsOfAGivenNode(firstArgument);
                Console.WriteLine(string.Join(", ", nodes.Select(x => x.Name)));
            }
            //5.counts all parents of a given node,
            if (instructionNumber == 5)
            {
                var nodes = Database.CountParentsOfAGivenNode(firstArgument);
                Console.WriteLine("Parents count: "+ nodes);
            }
            //6. Fnds all grand parents of a given node
            if (instructionNumber == 6)
            {
                var nodes = Database.FindGrandParentsOfAGivenNode(firstArgument);
                Console.WriteLine(string.Join(", ", nodes.Select(x => x.Name)));
            }


            if (instructionNumber == 99)
            {
               // Database.InitSqliteDatabase();
                //new Neo4jDatabase().InitNeo4jDatabase();
                new CsvDataLoader().LoadCsvFile(firstArgument != "" ? firstArgument : "C:\\Users\\asus\\Desktop\\code\\databases\\cli\\taxonomy_iw.csv");
            }


        }
    }
}
