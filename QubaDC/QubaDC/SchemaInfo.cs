using QubaDC.DatabaseObjects;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class SchemaInfo
    {
        public long? ID { get; set; }
        public Schema Schema { get; set; }
        public SchemaModificationOperator SMO { get; set; }

        public DateTime TimeOfCreation { get; set; }

    }
}
