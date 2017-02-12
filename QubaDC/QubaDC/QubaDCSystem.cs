using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.Separated;

namespace QubaDC
{
    public abstract class QubaDCSystem
    {
        public const String QubaDCSMOTable = "QubaDCSMOTable";

        public QubaDCSystem(DataConnection DataConnection, SMOVisitor separatedSMOHandler, CRUDVisitor separatedCRUDHandler)
        {
            this.DataConnection = DataConnection;
            this.SMOHandler = separatedSMOHandler;
            this.CRUDHandler = separatedCRUDHandler;
        }

        public void Init()
        {
            //Which tables do we need?
            //a.) Create SMO Tracking Table
            //b.) Create 
            CreateSMOTrackingTableIfNeeded();
            SetupQueryStore();
        }

        private void SetupQueryStore()
        {
            //throw new NotImplementedException();
        }

        private void CreateSMOTrackingTableIfNeeded()
        {

            if(!DataConnection.GetAllTables().Any(x=>x.Name== QubaDCSMOTable))
            {
                ;
            }
        }

        public SMOVisitor SMOHandler { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public CRUDVisitor CRUDHandler { get; private set; }
    }
}
