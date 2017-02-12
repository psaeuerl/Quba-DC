using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.Separated;
using QubaDC.Utility;

namespace QubaDC
{
    public abstract class QubaDCSystem
    {
        public const String QubaDCSMOTable = "QubaDCSMOTable";

        public readonly static String[] QubaDCSMOColumns = new string[]
        {
            "ID",
            "Schema",
            "SMO",
            "Timestamp"
        };

        public QubaDCSystem(DataConnection DataConnection, SMOVisitor separatedSMOHandler, CRUDVisitor separatedCRUDHandler, QueryStore qs)
        {
            this.DataConnection = DataConnection;
            this.SMOHandler = separatedSMOHandler;
            this.CRUDHandler = separatedCRUDHandler;
            this.QueryStore = qs;
        }

        public void Init()
        {
            //Which tables do we need?
            //a.) Create SMO Tracking Table           
            CreateSMOTrackingTableIfNeeded();
            //b.) SetupQueryStore
            SetupQueryStore();
        }

        private void SetupQueryStore()
        {            
            this.QueryStore.Init();
        }

        private void CreateSMOTrackingTableIfNeeded()
        { 
            if(!DataConnection.GetAllTables().Any(x=>x.Name== QubaDCSMOTable))
            {
                String sql = GetCreateSMOTrackingTableStatement();
                Guard.ArgumentTrueForAll<String>(QubaDCSMOColumns, (x) => { return sql.Contains(x); }, "SMO Table Columns");
                this.DataConnection.ExecuteNonQuerySQL(sql);
            }
        }

        protected abstract string GetCreateSMOTrackingTableStatement();
      

        public SMOVisitor SMOHandler { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public CRUDVisitor CRUDHandler { get; private set; }
        public QueryStore QueryStore { get; private set; }
    }
}
