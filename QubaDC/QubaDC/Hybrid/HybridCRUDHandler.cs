using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using QubaDC.Separated.CRUD;
using System.Data;
using QubaDC.Hybrid.CRUD;

namespace QubaDC.Hybrid
{
    public class HybridCRUDHandler : CRUDVisitor
    {
        public override void HandleDeletOperation(DeleteOperation deleteOperation)
        {
            throw new NotImplementedException("Not Implemented");
            HybridDeleteHandler h = new HybridDeleteHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            h.HandleDelete(deleteOperation);
        }

        public override void HandleInsert(InsertOperation insertOperation)
        {
            throw new NotImplementedException("Not Implemented");
            HybridInsertHandler h = new HybridInsertHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            h.HandleInsert(insertOperation);
        }

        public override void HandleUpdateOperation(UpdateOperation c2)
        {
            HybridUpdateHandler h = new HybridUpdateHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            h.HandleUpdate(c2);
        }

        public override string RenderSelectOperation(SelectOperation selectOperation)
        {
            throw new NotImplementedException("Not Implemented");
            HybridSelectHandler h = new HybridSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleSelect(selectOperation, false);
            return select;
        }

        internal override string RenderHashSelect(SelectOperation newOperation)
        {
            throw new NotImplementedException("Not Implemented");
            HybridSelectHandler h = new HybridSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleSelect(newOperation, true);
            return select;
        }

        internal override string RenderHybridHashSelect(SelectOperation newOperation, SchemaInfo s,SchemaInfo s2, Dictionary<String, Guid?> TableRefToGuidMapping)
        {
            throw new NotImplementedException("Not Implemented");
            HybridSelectHandler h = new HybridSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleHybridSelect(newOperation, s, s2,true, TableRefToGuidMapping);
            return select;
        }

        internal override string RenderHybridSelectOperation(SelectOperation originalSelect, SchemaInfo s, SchemaInfo s2, Dictionary<String, Guid?> TableRefToGuidMapping)
        {
            throw new NotImplementedException("Not Implemented");
            HybridSelectHandler h = new HybridSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleHybridSelect(originalSelect, s,s2,false, TableRefToGuidMapping);
            return select;
        }

        //public override String RenderSelectOperation(SelectOperation selectOperation)
        //{
        //    SeparatedSelectHandler h = new SeparatedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
        //    String select = h.HandleSelect(selectOperation,false);
        //    return select;
        //}

        //public override void HandleInsert(InsertOperation insertOperation)
        //{

        //}

        //public override void HandleDeletOperation(DeleteOperation deleteOperation)
        //{
        //    SeparatedDeleteHandler h = new SeparatedDeleteHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
        //    h.HandleDelete(deleteOperation);
        //}

        //internal override string RenderHashSelect(SelectOperation newOperation)
        //{
        //    SeparatedSelectHandler h = new SeparatedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
        //    String select = h.HandleSelect(newOperation, true);
        //    return select;

        //}

        //public override void HandleUpdateOperation(UpdateOperation c2)
        //{
        //    SeparatedUpdateHandler h = new SeparatedUpdateHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
        //    h.HandleUpdate(c2);
        //}
    }
}
