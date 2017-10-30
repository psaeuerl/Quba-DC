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
        public ulong data_free { get; internal set; }

        public ulong ActualStatus()
        {
            return data_length + index_length - data_free;
        }

        public double ActualStatusKibi()
        {
            return Convert.ToDouble(ActualStatus()) / 1024.00;
        }

        public double ActualStatusMibi()
        {
            return ActualStatusKibi() / 1024.00;
        }

        public double ActualStatusGibi()
        {
            return ActualStatusMibi() / 1024.00;
        }
    }
}
