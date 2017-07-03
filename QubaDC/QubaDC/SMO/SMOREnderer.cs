using QubaDC.CRUD;
using QubaDC.DatabaseObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public abstract class SMORenderer
    {
        public CRUDRenderer CRUDRenderer { get; internal set; }

        public CRUDVisitor CRUDHandler { get; internal set; }

        public abstract String RenderCreateTable(CreateTable ct, Boolean RemoveAdditionalColumnInfos=false);

        public abstract string RenderCreateInsertTrigger(TableSchema createTable, TableSchema ctHistTable);
        internal abstract string RenderCreateDeleteTrigger(TableSchema createTable, TableSchema ctHistTable);
        internal abstract string RenderCreateUpdateTrigger(TableSchema createTable, TableSchema ctHistTable);
        internal abstract string RenderRenameTable(RenameTable renameTable);
        internal abstract string RenderDropTable(String Schema, String Table);

        internal abstract string RenderCopyTable(String schema, String tablename, String newschema, String newname);

        internal abstract string RenderDropColumns(string schema, string name, string[] columns);
        internal abstract string RenderInsertFromOneTableToOther(TableSchema table, TableSchema firstTableSchema, Restriction rc, string[] columns);
        internal abstract string RenderCopyTable(string schema, string name, string v);
        internal abstract string RenderInsertToTableFromSelect(TableSchema joinedTableSchema, string select);
    }
}
