using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Evaluation
{
    public class TableStatus
    {
        public string tablename { get; internal set; }
        public string engine { get; internal set; }
        public ulong rows { get; internal set; }
        public ulong avg_row_length { get; internal set; }
        public ulong data_length { get; internal set; }
        public ulong index_length { get; internal set; }
    }
}
