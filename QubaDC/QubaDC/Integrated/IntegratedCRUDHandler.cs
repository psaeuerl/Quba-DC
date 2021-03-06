﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using QubaDC.Separated.CRUD;
using System.Data;
using QubaDC.Hybrid.CRUD;
using QubaDC.Integrated.CRUD;

namespace QubaDC.Integrated
{
    public class IntegratedCRUDHandler : CRUDVisitor
    {

        public override void HandleDeletOperation(DeleteOperation deleteOperation)
        {
            IntegratedDeleteHandler h = new IntegratedDeleteHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer, this.MetaManager);
            h.HandleDelete(deleteOperation);
        }

        public override void HandleInsert(InsertOperation insertOperation)
        {
            IntegratedInsertHandler h = new IntegratedInsertHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer,this.MetaManager);
            h.HandleInsert(insertOperation);
        }

        public override void HandleUpdateOperation(UpdateOperation c2)
        {
            IntegratedUpdateHandler h = new IntegratedUpdateHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer,this.MetaManager);
            h.HandleUpdate(c2);
        }

        public override string RenderSelectOperation(SelectOperation selectOperation)
        {
            IntegratedSelectHandler h = new IntegratedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleSelect(selectOperation, false);
            return select;
        }

        public override string RenderHashSelect(SelectOperation newOperation)
        {            
            IntegratedSelectHandler h = new IntegratedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleSelect(newOperation, true);
            return select;
        }

        public override string RenderHybridHashSelect(SelectOperation originalSelect, SchemaInfo s, SchemaInfo s2, Dictionary<String, Guid?> TableRefToGuidMapping)
        {
            IntegratedSelectHandler h = new IntegratedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleIntegratedSelect(originalSelect, s, s2, true, TableRefToGuidMapping);
            return select;
        }

        public override string RenderHybridSelectOperation(SelectOperation originalSelect, SchemaInfo s, SchemaInfo s2, Dictionary<String, Guid?> TableRefToGuidMapping)
        {
            IntegratedSelectHandler h = new IntegratedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String select = h.HandleIntegratedSelect(originalSelect, s, s2, false, TableRefToGuidMapping);
            return select;
        }
    }
}
