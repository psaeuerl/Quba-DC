using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public class RenameTable : SchemaModificationOperator
    {
        public String OldTableName { get; set; }

        public String OldSchema { get; set; }

        public String NewTableName { get; set; }

        public String NewSchema { get; set; }

        public override void Accept(SMOVisitor visitor)
        {
            visitor.Visit(this);
        }
    }
}
