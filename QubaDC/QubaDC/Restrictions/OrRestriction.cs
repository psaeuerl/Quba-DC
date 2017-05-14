using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Restrictions
{
    public class OrRestriction : Restriction
    {
        public Restriction[] Restrictions { get; set; }
    }
}
