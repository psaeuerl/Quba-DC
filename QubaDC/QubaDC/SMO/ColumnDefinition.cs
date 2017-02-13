using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public class ColumnDefinition
    {
        public String ColumName { get; set; }

        public String DataType { get; set; }

        public Boolean Nullable { get; set; }

        public String AdditionalInformation { get; set; }
    }
}
