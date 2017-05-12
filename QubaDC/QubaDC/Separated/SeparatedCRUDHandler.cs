using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using QubaDC.Separated.CRUD;

namespace QubaDC.Separated
{
    public class SeparatedCRUDHandler : CRUDVisitor
    {

        internal override void Visit(SelectOperation selectOperation)
        {
            SeparatedSelectHandler h = new SeparatedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            h.HandleSelect(selectOperation);
        }

        internal override void Visit(InsertOperation insertOperation)
        {
            SeparatedInsertHandler h = new SeparatedInsertHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            h.HandleInsert(insertOperation);
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
