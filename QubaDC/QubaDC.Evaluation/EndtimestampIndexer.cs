using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Evaluation
{
    public class EndtimestampIndexer
    {
        public void AddEndTimestampIndex(SystemSetup setup)
        {
            string tableForendtimestamp = null;
            if (setup.name == "Simple")
                return;
            if (setup.name == "Hybrid" || setup.name == "Separated")
                tableForendtimestamp = "datatable_1";
            else
                tableForendtimestamp = "datatable";
            String indexStmt = String.Format("CREATE INDEX endtsindex ON {0}(endts)", tableForendtimestamp);
            setup.quba.DataConnection.ExecuteNonQuerySQL(indexStmt);
        }
    }
}
