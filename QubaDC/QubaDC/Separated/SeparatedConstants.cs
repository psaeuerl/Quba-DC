using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Separated
{
    public class SeparatedConstants
    {

        public static IEnumerable<ColumnDefinition> GetHistoryTableColumns()
        {
            return new ColumnDefinition[]
            {
                 new ColumnDefinition() { ColumName="startts", DataType ="DATETIME(6)", Nullable = false },
                 new ColumnDefinition() { ColumName="endts", DataType ="DATETIME(6)", Nullable = true },
                 new ColumnDefinition() { ColumName="guid", DataType ="char(36)", Nullable = false }
            };
        }
    }
}
