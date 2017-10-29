using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using QubaDC.Separated.CRUD;
using System.Data;
using QubaDC.Evaluation.SimpleSystem;

namespace QubaDC.SimpleSystem
{
    public class SimpleSystemCRUDHandler : CRUDVisitor
    {

        //public override String RenderSelectOperation(SelectOperation selectOperation)
        //{
        //    SeparatedSelectHandler h = new SeparatedSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
        //    String select = h.HandleSelect(selectOperation,false);
        //    return select;
        //}

        //public override void HandleInsert(InsertOperation insertOperation)
        //{
        //    SeparatedInsertHandler h = new SeparatedInsertHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer,this.MetaManager);
        //    h.HandleInsert(insertOperation);
        //}

        //public override void HandleDeletOperation(DeleteOperation deleteOperation)
        //{
        //    SeparatedDeleteHandler h = new SeparatedDeleteHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer,this.MetaManager);
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
        //    SeparatedUpdateHandler h = new SeparatedUpdateHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer, this.MetaManager);
        //    h.HandleUpdate(c2);
        //}

        //public override string RenderHybridSelectOperation(SelectOperation originalSelect, SchemaInfo executiontimeSchema, SchemaInfo currentSchema, Dictionary<string, Guid?> TableRefToGuidMapping)
        //{
        //    throw new NotImplementedException();
        //}

        //public override string RenderHybridHashSelect(SelectOperation newOperation, SchemaInfo executiontimeSchema, SchemaInfo currentSchema, Dictionary<string, Guid?> TableRefToGuidMapping)
        //{
        //    throw new NotImplementedException();
        //}
        public override void HandleDeletOperation(DeleteOperation deleteOperation)
        {
            throw new NotImplementedException();
        }

        public override void HandleInsert(InsertOperation insertOperation)
        {
            String insert = this.CRUDRenderer.RenderInsert(insertOperation.InsertTable, insertOperation.ColumnNames, insertOperation.ValueLiterals);
            this.DataConnection.ExecuteInsert(insert);            
        }

        public override void HandleUpdateOperation(UpdateOperation c2)
        {
            String update = this.CRUDRenderer.RenderUpdate(c2.Table, c2.ColumnNames, c2.ValueLiterals, c2.Restriction);
            this.DataConnection.ExecuteNonQuerySQL(update);
        }

        public override string RenderSelectOperation(SelectOperation selectOperation)
        {
            SimpleSelectHandler s = new SimpleSelectHandler(this.DataConnection, this.SchemaManager, this.CRUDRenderer);
            String result = s.HandleSelect(selectOperation, false);
            return result;            
        }

        public override string RenderHashSelect(SelectOperation newOperation)
        {
            throw new NotImplementedException();
        }

        public override string RenderHybridHashSelect(SelectOperation newOperation, SchemaInfo executiontimeSchema, SchemaInfo currentSchema, Dictionary<string, Guid?> TableRefToGuidMapping)
        {
            throw new NotImplementedException();
        }

        public override string RenderHybridSelectOperation(SelectOperation originalSelect, SchemaInfo executiontimeSchema, SchemaInfo currentSchema, Dictionary<string, Guid?> TableRefToGuidMapping)
        {
            throw new NotImplementedException();
        }
    }
}
