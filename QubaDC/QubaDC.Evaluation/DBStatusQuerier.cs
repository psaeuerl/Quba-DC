using QubaDC.Evaluation.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Evaluation
{
    public class DBStatusQuerier
    {
        public MySQLDataConnection Connection { get; set; }

        public void getDBSize(String dbname)
        {
            String query = String.Format(@"SELECT  sum(round(((data_length + index_length) / 1024  ), 2))  as sizekb
FROM information_schema.TABLES
WHERE table_schema = '{0}'", dbname);
            DataTable t = Connection.ExecuteQuery(query);
            foreach (var row in t.Select())
            {
                Decimal sizeKB = row.Field<Decimal>("sizekb");


                Output.WriteLine("----------------");
                Output.WriteLine(String.Format("DB: {0}, SizeInKB: {1}", dbname, sizeKB));
                Output.WriteLine("----------------");
            }

        }

    }
}
