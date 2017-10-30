using QubaDC.Evaluation.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Evaluation
{
    public class TableSizeQuerier
    {
        public MySQLDataConnection Connection { get; set; }

        public Dictionary<String, TableStatus> GetDataTableStatus(String dbname)
        {

             Connection.UseDatabase(dbname);
            var allTables = Connection.GetAllTables();
            TableFlusher tFlusher = new TableFlusher();
            tFlusher.flushTables(Connection, allTables.Select(x => x.Name), dbname, new Dictionary<string, ulong>());
            


             String query = "SHOW TABLE STATUS IN " + dbname;
            //String qFormat = " select * from INFORMATION_SCHEMA.INNODB_SYS_TABLESPACES where name='{0}/{1}'";

            DataTable t = Connection.ExecuteQuery(query);
            Dictionary<String, TableStatus> tblStatus = new Dictionary<string, TableStatus>();
            foreach (var row in t.Select())
            {
                tblStatus.Add(
                     row.Field<String>("Name"),
                     new TableStatus()
                     {
                         tablename = row.Field<String>("Name"),
                         engine = row.Field<String>("Engine"),
                         rows = row.Field<ulong>("Rows"),
                         avg_row_length = row.Field<ulong>("Avg_row_length"),
                         data_length = row.Field<ulong>("Data_length"),
                         index_length = row.Field<ulong>("Index_length"),
                         data_free = row.Field<ulong>("Data_free")
                     }
                    );

            }
            return tblStatus;
        }


        public void printTableStatus(String dbname)
        {
            var stati = GetDataTableStatus(dbname);
            foreach (var x in stati.Values)
            {
                Output.WriteLine("----------------");
                Output.WriteLine(String.Format("Table: {0}, Engine: {1}, Rows: {2}", x.tablename, x.engine, x.rows));
                Output.WriteLine("  Rows: \t\t" + x.rows);
                Output.WriteLine("  Avg_Row_Length:\t" + x.avg_row_length);
                Output.WriteLine("  Data_length:\t\t" + x.data_length);
                Output.WriteLine("  Index_length:\t\t" + x.index_length);
                Output.WriteLine("  Data_Free:\t\t" + x.data_free);
                Output.WriteLine("  Actual Storage:\t\t" + x.ActualStatus());
                Output.WriteLine("  Actual Storage Kibi:\t\t" + x.ActualStatusKibi());
                Output.WriteLine("  Actual Storage Mibi:\t\t" + x.ActualStatusMibi());
                Output.WriteLine("  Actual Storage Gibi:\t\t" + x.ActualStatusGibi());
                Output.WriteLine("----------------");
            }

            Output.WriteLine("----DB Sum-----");
            var sumActualKibi = stati.Values.Select(x => (int)x.ActualStatusKibi()).Sum();
            var sumActualMibi = stati.Values.Select(x => (int)x.ActualStatusMibi()).Sum();
            var sumActualGibi = stati.Values.Select(x => (int)x.ActualStatusGibi()).Sum();
            Output.WriteLine("Actual Kibi: \t" + sumActualKibi);
            Output.WriteLine("Actual Mibi: \t" + sumActualMibi);
            Output.WriteLine("Actual Gibi: \t" + sumActualGibi);
        }

    }
}
