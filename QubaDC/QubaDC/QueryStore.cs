using QubaDC.Utility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;

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

        private QueryStoreSelectHandler SelectHandler { get; set; }

        public SchemaManager SchemaManager { get; set; }

        public QueryStore(DataConnection dataConnection,QueryStoreSelectHandler handler)
        {
            this.DataConnection = dataConnection;
            this.SelectHandler = handler;
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

        public object HandleSelect(SelectOperation s)
        {
            //What to do here?
            //FROM Identification of Reproducible Subsets for Data Citation, Sharing and Re-Use
            //1.Ensure stable sorting and normalise query(R5). 
            //2.Compute query hash (R4). 
            //3.Open transaction and lock tables(I)
            //4.Execute(original) query and retrieve subset.
            //5.Assign the last global update timestamp to the query (R7).
            //6.Close transaction and unlock tables(A) 
            //7.Compute result set verication hash(R6). 
            //8.Decision process: (a)Decide if the query requires a new PID
            //(R8).If so: (b)Persist metadata and query(R9)

            //1.Ensure stable sorting and normalise query(R5). 
            EnsureSorting(s);
            //2.Compute query hash (R4). 
            //Open if needed .... really ... query is stored completly
            //3-4-5-6 handeld here
            SelectHandler.HandleSelect(s, SchemaManager,DataConnection);
            //7.Compute result set verication hash(R6). 
            GenerteResultHash();

            //8.Decision process: 
            //(a)Decide if the query requires a new PID (R8)
            //If so: (b)Persist metadata and query     (R9)
            //Approach here => always store
            StoreResult();
            return null;
        }

        private void StoreResult()
        {
            throw new NotImplementedException();
        }

        private void GenerteResultHash()
        {
            throw new NotImplementedException();
        }

        private void EnsureSorting(SelectOperation s)
        {
            ;//TODO
        }
    }
}
