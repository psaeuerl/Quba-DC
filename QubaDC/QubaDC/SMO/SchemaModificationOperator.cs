using QubaDC;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public abstract class SchemaModificationOperator
    {
        public abstract void Accept(SMOVisitor visitor);
    }
}
