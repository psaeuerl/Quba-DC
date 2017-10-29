using System; 
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace QubaDC.Evaluation
{
    class TableFlusher
    {

        public void flushTables(DataConnection c, IEnumerable<String>  tablename, String dbname, Dictionary<String,ulong> expectedRowValues)
        {
            Boolean correctResult = true;
            int sleepseconds = 1;
            do
            {
                Thread.Sleep(sleepseconds * 1000);
                String flushTables = String.Format(@"FLUSH TABLES {0}", String.Join(",", tablename));
                c.ExecuteNonQuerySQL(flushTables);
                correctResult = true;
                Thread.Sleep(sleepseconds * 1000);
                ////Why Analyze Table?
                ////https://stackoverflow.com/questions/33383877/why-does-information-schema-tables-give-such-an-unstable-answer-for-number-of-ro
                ////Acutally updates the information_schema table and we get correct results
                String analyze = String.Format(@"ANALYZE TABLE {0}", String.Join(",", tablename));
                var result = c.ExecuteQuery(analyze);
                Thread.Sleep(sleepseconds * 1000);
            } while (!correctResult);
        }
    }
}
