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
        public const String GlobalUpdateTableName = "QubaDCGlobalUpdate";        

        public readonly static String[] QubaDCSMOColumns = new string[]
        {
            "ID",
            "Schema",
            "SMO",
            "Timestamp"
        };

        public QubaDCSystem(DataConnection DataConnection, SMOVisitor separatedSMOHandler, 
            CRUDVisitor separatedCRUDHandler, 
            QueryStore qs, SchemaManager manager, SMORenderer renderer, CRUDRenderer r, GlobalUpdateTimeManager globalTimeManager)
        {
            this.DataConnection = DataConnection;
            this.SMOHandler = separatedSMOHandler;
            this.CRUDHandler = separatedCRUDHandler;
            this.QueryStore = qs;
            this.SchemaManager = manager;
            this.SMORenderer = renderer;
            this.CRUDRenderer = r;
            this.GlobalUpdateTimeManager = globalTimeManager;

            qs.SchemaManager = manager;
            qs.TimeManager = globalTimeManager;
            qs.CRUDHandler = separatedCRUDHandler;

            renderer.CRUDRenderer = r;
            renderer.CRUDHandler = separatedCRUDHandler;

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
            this.DataConnection.DoTransaction((transaction, c) =>
            {
                if (!DataConnection.GetAllTables().Any(x => x.Name == GlobalUpdateTableName))
                {
                    String sql = this.GlobalUpdateTimeManager.GetCreateUpdateTimeTableStatement();
                    this.DataConnection.ExecuteNonQuerySQL(sql, c);
                }

                if (!DataConnection.GetAllTables().Any(x => x.Name == QubaDCSMOTable))
                {
                    String sql = this.SchemaManager.GetCreateSchemaStatement();
                    Guard.ArgumentTrueForAll<String>(QubaDCSMOColumns, (x) => { return sql.Contains(x); }, "SMO Table Columns");
                    this.DataConnection.ExecuteNonQuerySQL(sql, c);

                    String trigger = this.SchemaManager.GetInsertToGlobalUpdateTrigger();
                    this.DataConnection.ExecuteSQLScript(trigger, c);
                    this.SchemaManager.StoreSchema(new Schema(), null, this.DataConnection, c);

                    transaction.Commit();
                }
            });
        }
    

        public SMOVisitor SMOHandler { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public CRUDVisitor CRUDHandler { get; private set; }
        public QueryStore QueryStore { get; private set; }
        public SchemaManager SchemaManager { get; private set; }
        public SMORenderer SMORenderer { get; private set; }
        public CRUDRenderer CRUDRenderer { get; private set; }
        public GlobalUpdateTimeManager GlobalUpdateTimeManager { get; private set; }
    }
}
