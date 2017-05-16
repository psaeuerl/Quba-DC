using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.Restrictions;

namespace QubaDC
{
    public class AndRestriction : Restriction
    {
        public Restriction[] Restrictions { get; set; }

        public override T Accept<T>(RestrictionTreeTraverser<T> visitor)
        {
            return visitor.Visit(this);
        }
    
    }
}
