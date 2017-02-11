using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;

namespace QubaDC.Separated
{
    public class SeparatedCRUDHandler : CRUDVisitor
    {
        internal override void Visit(SelectOperation selectOperation)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(InsertOperation insertOperation)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(UpdateOperation updateOperation)
        {
            throw new NotImplementedException();
        }

        internal override void Visit(DeleteOperation deleteOperation)
        {
            throw new NotImplementedException();
        }
    }
}
