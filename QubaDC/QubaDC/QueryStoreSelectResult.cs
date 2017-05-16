using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class QueryStoreSelectResult
    {

        public String RewrittenSerialized { get; set; }
        public String RewrittenRenderd { get; set; }
        public DateTime TimeStampOfExecution { get; set; }

        public DataTable Result { get; set; }
        public string Hash { get; internal set; }
        public Guid GUID { get; internal set; }
    }
}
