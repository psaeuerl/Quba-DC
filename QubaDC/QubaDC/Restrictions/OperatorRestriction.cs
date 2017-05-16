using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Restrictions
{
    public class OperatorRestriction : Restriction
    {
        public RestrictionOperand LHS { get; set; }
        public RestrictionOperator Op { get; set; }
        public RestrictionOperand RHS { get; set; }

        public override T Accept<T>(RestrictionTreeTraverser<T> visitor)
        {
            return visitor.Visit(this);
        }
    }
}
