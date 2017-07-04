using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public class AddColum : SchemaModificationOperator
    {
        public String TableName { get; set; }

        public String Schema { get; set; }

        public ColumnDefinition Column { get; set; }

        public String InitalValue { get; set; }

        public override void Accept(SMOVisitor visitor)
        {
            visitor.Visit(this);
        }
    }
}
