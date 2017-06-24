using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public class PartitionTable : SchemaModificationOperator
    {
        public String BaseTableName { get; set; }

        public String BaseSchema { get; set; }

        public String TrueConditionTableName { get; set; }

        public String TrueConditionSchema { get; set; }

        public String FalseConditionTableName { get; set; }

        public String FalseConditionSchema { get; set; }

        public Restriction Restriction { get; set; } = null;

        public override void Accept(SMOVisitor visitor)
        {
            visitor.Visit(this);
        }
    }
}
