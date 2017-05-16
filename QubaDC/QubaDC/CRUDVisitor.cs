using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using System.Data;

namespace QubaDC
{
    public abstract class CRUDVisitor
    {
        public CRUDRenderer CRUDRenderer { get; set; }
        public DataConnection DataConnection { get; set; }
        public SchemaManager SchemaManager { get; set; }

        public abstract String Visit(DeleteOperation deleteOperation);

        public abstract String Visit(UpdateOperation updateOperation);


        public abstract String RenderSelectOperation(SelectOperation selectOperation);

        public abstract DataTable ExecuteSelectOperaiton(SelectOperation sel);



        public abstract void HandleInsert(InsertOperation insertOperation);
        internal abstract string RenderHashSelect(SelectOperation newOperation);


    }
}
