using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;

namespace QubaDC
{
    public class CRUDVisitor
    {
        internal void Visit(DeleteOperation deleteOperation)
        {
            throw new NotImplementedException();
        }

        internal void Visit(UpdateOperation updateOperation)
        {
            throw new NotImplementedException();
        }

        internal void Visit(SelectOperation selectOperation)
        {
            throw new NotImplementedException();
        }

        internal void Visit(InsertOperation insertOperation)
        {
            throw new NotImplementedException();
        }

        public void Visit(CRUDOperation crudOP)
        {
            crudOP.Accept(this);
        }
    }
}
