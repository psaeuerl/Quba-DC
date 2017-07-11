using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public class RenameColumn : SchemaModificationOperator
    {
        public String TableName { get; set; }

        public String Schema { get; set; }

        public String ColumnName { get; set; }

        public String RenameName { get; set; }

        public override void Accept(SMOVisitor visitor)
        {
            visitor.Visit(this);
        }
    }
}
