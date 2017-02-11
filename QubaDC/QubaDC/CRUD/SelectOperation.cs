using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.CRUD
{
    public class SelectOperation : CRUDOperation
    {
        //TODO
        public override void Accept(CRUDVisitor visitor)
        {
            visitor.Visit(this);
        }
    }
}
