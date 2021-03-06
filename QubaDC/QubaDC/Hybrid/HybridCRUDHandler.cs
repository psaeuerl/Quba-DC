﻿using System;
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
            HybridDeleteHandler h = new HybridDeleteHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer, this.MetaManager);
            h.HandleDelete(deleteOperation);
        }

        public override void HandleInsert(InsertOperation insertOperation)
        {            
            HybridInsertHandler h = new HybridInsertHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer,this.MetaManager);
            h.HandleInsert(insertOperation);
        }

        public override void HandleUpdateOperation(UpdateOperation c2)
        {
            HybridUpdateHandler h = new HybridUpdateHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer, this.MetaManager);
            h.HandleUpdate(c2);
        }

        public override string RenderSelectOperation(SelectOperation selectOperation)
        {
            HybridSelectHandler h = new HybridSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleSelect(selectOperation, false);
            return select;
        }

        public override string RenderHashSelect(SelectOperation newOperation)
        {
            HybridSelectHandler h = new HybridSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleSelect(newOperation, true);
            return select;
        }

        public override string RenderHybridHashSelect(SelectOperation newOperation, SchemaInfo s,SchemaInfo s2, Dictionary<String, Guid?> TableRefToGuidMapping)
        {
            HybridSelectHandler h = new HybridSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleHybridSelect(newOperation, s, s2,true, TableRefToGuidMapping);
            return select;
        }

        public override string RenderHybridSelectOperation(SelectOperation originalSelect, SchemaInfo s, SchemaInfo s2, Dictionary<String, Guid?> TableRefToGuidMapping)
        {
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

        //public override string RenderHashSelect(SelectOperation newOperation)
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
