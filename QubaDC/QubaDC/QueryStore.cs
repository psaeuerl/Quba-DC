using QubaDC.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public abstract class QueryStore
    {
        public const String QueryStoreTable = "QueryStore";
        public static readonly String[] QueryStoreTableColumns = new String[]
        {
            "ID",
            "Query",
            "ReWrittenQuery",
            "Timestamp",
            "Checkvalue"
        };

        private DataConnection DataConnection;

        public QueryStore(DataConnection dataConnection)
        {
            this.DataConnection = dataConnection;
        }

        public void Init()
        {
            if (!this.DataConnection.GetAllTables().Select(x => x.Name.ToLower()).Any(x => x == QueryStoreTable))
            {
                //Create Table
                String sql = this.GetCreateQueryStoreTableStatement();
                Guard.ArgumentTrueForAll<String>(QueryStoreTableColumns, (x) => { return sql.Contains(x); }, "SMO Table Columns");
                this.DataConnection.ExecuteNonQuerySQL(sql);
            }
        }

        protected abstract string GetCreateQueryStoreTableStatement();
    }
}
