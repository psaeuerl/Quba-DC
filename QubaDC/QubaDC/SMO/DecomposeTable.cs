using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public class DecomposeTable : SchemaModificationOperator
    {
        public String BaseTableName { get; set; }

        public String BaseSchema { get; set; }

        public String FirstTableName { get; set; }

        public String FirstSchema { get; set; }

        public String[] FirstColumns { get; set; }

        public String SecondTableName { get; set; }

        public String SecondSchema { get; set; }

        public String[] SecondColumns { get; set; }

        public String[] SharedColumns { get; set; }

        public override void Accept(SMOVisitor visitor)
        {
            visitor.Visit(this);
        }
    }
}
