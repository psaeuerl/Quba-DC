﻿using System;
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
            String select = h.HandleSelect(selectOperation,false);
            return select;
        }

        public override void HandleInsert(InsertOperation insertOperation)
        {
            SeparatedInsertHandler h = new SeparatedInsertHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            h.HandleInsert(insertOperation);
        }

        public override void HandleDeletOperation(DeleteOperation deleteOperation)
        {
            SeparatedDeleteHandler h = new SeparatedDeleteHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            h.HandleDelete(deleteOperation);
        }

        internal override string RenderHashSelect(SelectOperation newOperation)
        {
            SeparatedSelectHandler h = new SeparatedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleSelect(newOperation, true);
            return select;

        }

        public override void HandleUpdateOperation(UpdateOperation c2)
        {
            SeparatedUpdateHandler h = new SeparatedUpdateHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            h.HandleUpdate(c2);
        }
    }
}
