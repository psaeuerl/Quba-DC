using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public abstract class SMORenderer
    {
        public abstract String RenderCreateTable(CreateTable ct, Boolean RemoveAdditionalColumnInfos=false);

        public abstract IEnumerable<string> GetHistoryTableColumns();
    }
}
