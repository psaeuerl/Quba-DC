using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using QubaDC.Separated.CRUD;
using System.Data;

namespace QubaDC.Separated
{
    public class SeparatedCRUDHandler : CRUDVisitor
    {

        public override String RenderSelectOperation(SelectOperation selectOperation)
        {
            SeparatedSelectHandler h = new SeparatedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleSelect(selectOperation);
            return select;
        }

        public override DataTable ExecuteSelectOperaiton(SelectOperation sel)
        {
            throw new NotImplementedException();
        }

        public override void HandleInsert(InsertOperation insertOperation)
        {
            SeparatedInsertHandler h = new SeparatedInsertHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            h.HandleInsert(insertOperation);
        }

        public override String Visit(UpdateOperation updateOperation)
        {
            throw new NotImplementedException();
        }

        public override String Visit(DeleteOperation deleteOperation)
        {
            throw new NotImplementedException();
        }
    }
}
