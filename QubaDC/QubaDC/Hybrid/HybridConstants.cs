using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Hybrid
{
    public class HybridConstants
    {
        public const String StartTS = "startts";
        public const String EndTS = "endts";

        public static ColumnDefinition GetStartColumn()
        {
            return new ColumnDefinition() { ColumName = StartTS, DataType = "DATETIME(3)", Nullable = false };
        }

        public static ColumnDefinition GetEndColumn()
        {
            return new ColumnDefinition() { ColumName = EndTS, DataType = "DATETIME(3)", Nullable = true };
        }


    }
}
