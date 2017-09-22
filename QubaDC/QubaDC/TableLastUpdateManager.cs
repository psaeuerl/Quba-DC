using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;
using QubaDC.DatabaseObjects;

namespace QubaDC
{
    public abstract class TableMetadataManager
    {
        public abstract String GetCreateUpdateTimeTableStatement();
        public abstract TableLastUpdate GetLatestUpdate();
        public abstract string GetTableName();
        public abstract Table GetTable();
        internal abstract string GetCreateMetaTableFor(string schema, string tableName);
        internal abstract Table GetMetaTableFor(string schema, string tableName);
        internal abstract string GetStartInsertFor(string schema, string tableName);

        public abstract DateTime GetLatestUpdate(params Table[] tables);
        internal abstract bool GetCanBeQueriedFor(Table changingTable, DbConnection con, Action<String> log);
        internal abstract string GetSetLastUpdateStatement(Table insertTable, string v);
        internal abstract Table GetMetaTableFor(TableSchema copiedTableSchema);
        internal abstract string SetCanBeQueriedFalse(Table x);
        internal abstract string SetCanBeQueriedTrue(Table x);
    }
}
