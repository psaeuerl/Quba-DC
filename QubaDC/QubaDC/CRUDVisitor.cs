using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;

namespace QubaDC
{
    public abstract class CRUDVisitor
    {
        internal abstract void Visit(DeleteOperation deleteOperation);

        internal abstract void Visit(UpdateOperation updateOperation);


        internal abstract void Visit(SelectOperation selectOperation);


        internal abstract void Visit(InsertOperation insertOperation);


        public void Visit(CRUDOperation crudOP)
        {
            crudOP.Accept(this);
        }
    }
}
