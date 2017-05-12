using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.Separated;
using QubaDC.Utility;
using QubaDC.SMO;
using QubaDC.CRUD;
using QubaDC.DatabaseObjects;

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

        public QubaDCSystem(DataConnection DataConnection, SMOVisitor separatedSMOHandler, 
            CRUDVisitor separatedCRUDHandler, 
            QueryStore qs, SchemaManager manager, SMORenderer renderer, CRUDRenderer r)
        {
            this.DataConnection = DataConnection;
            this.SMOHandler = separatedSMOHandler;
            this.CRUDHandler = separatedCRUDHandler;
            this.QueryStore = qs;
            this.SchemaManager = manager;
            this.SMORenderer = renderer;
            this.CRUDRenderer = r;

            SMOHandler.DataConnection = this.DataConnection;
            SMOHandler.SchemaManager = this.SchemaManager;
            SMOHandler.SMORenderer = this.SMORenderer;

            CRUDHandler.CRUDRenderer = this.CRUDRenderer;
            CRUDHandler.SchemaManager = this.SchemaManager;
            CRUDHandler.DataConnection = this.DataConnection;
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

        public void CreateSMOTrackingTableIfNeeded()
        { 
            if(!DataConnection.GetAllTables().Any(x=>x.Name== QubaDCSMOTable))
            {
                String sql = this.SchemaManager.GetCreateSchemaStatement();
                Guard.ArgumentTrueForAll<String>(QubaDCSMOColumns, (x) => { return sql.Contains(x); }, "SMO Table Columns");
                this.DataConnection.ExecuteNonQuerySQL(sql);
                String insert = this.SchemaManager.GetInsertSchemaStatement(new Schema(), null);
                this.DataConnection.ExecuteNonQuerySQL(insert);
            }
        }
    

        public SMOVisitor SMOHandler { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public CRUDVisitor CRUDHandler { get; private set; }
        public QueryStore QueryStore { get; private set; }
        public SchemaManager SchemaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }
        public CRUDRenderer CRUDRenderer { get; private set; }
    }
}
