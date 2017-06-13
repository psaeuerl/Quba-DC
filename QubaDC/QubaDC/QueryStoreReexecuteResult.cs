using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public class QueryStoreReexecuteResult
    {
        public Guid GUID { get; set; }
        public String Hash { get; set; }
        public DataTable Result { get; set; }
    }
}
