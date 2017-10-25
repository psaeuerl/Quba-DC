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
        public CRUDRenderer CRUDRenderer { get;  set; }

        public CRUDVisitor CRUDHandler { get;  set; }

        public abstract String RenderCreateTable(CreateTable ct, Boolean RemoveAdditionalColumnInfos=false);

        public abstract string RenderCreateInsertTrigger(TableSchema createTable, TableSchema ctHistTable);
        public abstract string RenderCreateDeleteTrigger(TableSchema createTable, TableSchema ctHistTable);
        public abstract string RenderCreateUpdateTrigger(TableSchema createTable, TableSchema ctHistTable);
        public abstract string RenderRenameTable(RenameTable renameTable);
        public abstract string RenderDropTable(String Schema, String Table);

        public abstract string RenderCopyTable(String schema, String tablename, String newschema, String newname);

        public abstract string RenderDropColumns(string schema, string name, string[] columns);
        public abstract string RenderInsertFromOneTableToOther(TableSchema table, TableSchema firstTableSchema, Restriction rc, string[] selectColumns,string[] insertcolumns = null, string[] literals = null);
        public abstract string RenderCopyTable(string schema, string name, string select);
        public abstract string RenderInsertToTableFromSelect(TableSchema joinedTableSchema, string select);

        public abstract string RenderAddColumn(TableSchema copiedTableSchema, ColumnDefinition column);
        public abstract string RenderDropInsertTrigger(TableSchema copiedTableSchema, TableSchema ctHistTable);
        public abstract string RenderDropUpdaterigger(TableSchema copiedTableSchema, TableSchema ctHistTable);
        public abstract string RenderDropDeleteTrigger(TableSchema copiedTableSchema, TableSchema ctHistTable);
        public abstract string RenderRenameColumn(RenameColumn renameColumn, ColumnDefinition cd, TableSchema schema);
    }
}
