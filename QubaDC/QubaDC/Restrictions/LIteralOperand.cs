using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Restrictions
{
    public class LiteralOperand : RestrictionOperand
    {
        public String Literal { get; set; }
        public override T Accept<T>(RestrictionTreeTraverser<T> visitor)
        {
            return visitor.Visit(this);
        }

    }
}
