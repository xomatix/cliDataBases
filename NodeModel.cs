using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace cli
{
    public class NodeModel
    {
        public NodeModel() { }
        public int ID { get; set; }
        public string Name { get; set; } = "";
        public List<int> childrenIds { get; set; } = new List<int>();
        public string childrenIdsString { get; set; }
        public int childrenCount { get; set; }
    }
}
