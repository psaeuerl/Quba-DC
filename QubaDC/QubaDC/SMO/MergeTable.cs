using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public class MergeTable : SchemaModificationOperator
    {
        public String ResultTableName { get; set; }

        public String ResultSchema { get; set; }

        public String FirstTableName { get; set; }

        public String FirstSchema { get; set; }

        public String SecondTableName { get; set; }

        public String SecondSchema { get; set; }
        public override void Accept(SMOVisitor visitor)
        {
            visitor.Visit(this);
        }
    }
}
