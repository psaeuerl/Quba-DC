using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.CRUD
{
    public abstract class CRUDRenderer
    {
        public abstract string RenderInsert(Table insertTable, string[] columnNames, string[] valueLiterals);

        public abstract string SerializeDateTime(DateTime now);
        internal abstract string SerializeString(string v);
    }
}
