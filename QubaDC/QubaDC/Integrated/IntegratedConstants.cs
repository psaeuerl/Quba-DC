using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated
{
    public class IntegratedConstants
    {
        public const String StartTS = "startts";
        public const String EndTS = "endts";

        public static ColumnDefinition[] GetHistoryTableColumns()
        {
            return new ColumnDefinition[]
            {
                 new ColumnDefinition() { ColumName=StartTS, DataType ="DATETIME(3)", Nullable = false },
                 new ColumnDefinition() { ColumName=EndTS, DataType ="DATETIME(3)", Nullable = true },
            };
        }
    }
}
