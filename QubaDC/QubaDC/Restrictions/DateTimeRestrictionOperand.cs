using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Restrictions
{
    public class DateTimeRestrictionOperand : RestrictionOperand
    {
        public DateTime Value { get; set; }
        public override T Accept<T>(RestrictionTreeTraverser<T> visitor)
        {
            return visitor.Visit(this);
        }

    }
}
