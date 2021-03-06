﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QubaDC.CRUD;

namespace QubaDC
{
    public class MySQLCrudRenderer : CRUDRenderer
    {
        public override string RenderInsert(Table insertTable, string[] columnNames, string[] valueLiterals)
        {            
            String InsertFormat = "INSERT INTO {0} ({3}{1}) VALUES ({3}{2})";
            String table = PrepareTable(insertTable);
            String columns = PrepareColumn(columnNames);
            String values = PrepareValues(valueLiterals);
            String result = String.Format(InsertFormat, table, columns, values,System.Environment.NewLine);
            return result;            
        }

        private String QualifyObjectName(String name)
        {
            return "`" + name + "`";
        }

        private string PrepareValues(string[] valueLiterals)
        {
            return String.Join("," + System.Environment.NewLine, valueLiterals);
        }

        public String RenderColumn(ColumnReference column)
        {
            return QualifyObjectName(column.TableReference) + "." + QualifyObjectName(column.ColumnName);
        }

        private string PrepareColumn(string[] columnNames)
        {
            var cols = columnNames.Select(x =>  QualifyObjectName(x.ToLowerInvariant()));
            return String.Join("," + System.Environment.NewLine, cols);
        }

        public override string PrepareTable(Table insertTable)
        {
            return QualifyObjectName(insertTable.TableSchema.ToLowerInvariant())
                +"."
                + QualifyObjectName(insertTable.TableName.ToLowerInvariant());
        }

        public override string SerializeDateTime(DateTime now)
        {
            return MySQLDialectHelper.RenderDateTime(now);
        }

        public override string SerializeString(string v)
        {
            return "'" + v + "'";
        }

        public override string RenderRestriction(Restriction joinCondition)
        {
            if (joinCondition == null)
                return "";
            MySQLRestrictionVisitor v = new MySQLRestrictionVisitor(this);
            String result = joinCondition.Accept<String>(v);
            return result;
        }

        public override string RenderDelete(Table table, Restriction restriction)
        {
            String restrictions = RenderRestriction(restriction);
            String restPart = String.IsNullOrWhiteSpace(restrictions) ? "" : "WHERE " + restrictions;
            String tableRef = PrepareTable(table);

            String baseDelete = "DELETE FROM {0} {1}";
            String deleteResult = String.Format(baseDelete, tableRef, restPart);
            return deleteResult;
        }

        public override string RenderUpdate(Table table, string[] columnNames, string[] valueLiterals, Restriction restriction)
        {
            String restrictions = RenderRestriction(restriction);
            String restPart = String.IsNullOrWhiteSpace(restrictions) ? "" : "WHERE " + restrictions;
            String tableRef = PrepareTable(table);

            String[] setValues = columnNames.Zip(valueLiterals, (x, y) => QualifyObjectName(x) + " = " + y).ToArray();
            String settingvalues = String.Join(","+System.Environment.NewLine, setValues);

            String baseUpdate = "UPDATE {0} SET {1} {2}";
            String updateResult = String.Format(baseUpdate, tableRef, settingvalues,restPart);
            return updateResult;
        }

        public override string RenderInsertSelect(Table insertTable, string[] columnnames, string select)
        {
           // String InsertFormat = "INSERT INTO {0} ({3}{1})  {2}";
            String InsertFormat = "INSERT INTO {0}  {2}";
            String table = PrepareTable(insertTable);
            String columns = "";// PrepareColumn(columnnames);
            String values = select;
            String result = String.Format(InsertFormat, table, columns, values, System.Environment.NewLine);
            return result;
        }

        public override string[] RenderLockTables(string[] locktables,Boolean[] lockAsWrite)
        {
            var lockWithWrite = locktables.Zip(lockAsWrite,(s,b) =>  new { table = s, write = b });
            String[] tablesWithWrite = lockWithWrite.Select(x => { return x.table + (x.write ? " WRITE" : " READ"); }).ToArray();
            String lockTables = String.Format("LOCK TABLES {0}", String.Join(",", tablesWithWrite)) + ";";
            String[] setupAndAquireLock = new String[] {               
                lockTables
            };
            return setupAndAquireLock;
        }

        public override string[] RenderAutoCommitZero()
        {
             return new String[] { @"SET autocommit=0;",
                "SET SQL_SAFE_UPDATES=0;" };
        }

        public override string[] RenderCommitAndUnlock()
        {
            return new String[]
            {
                "COMMIT;",
                "UNLOCK TABLES;"
            };
        }

        public override string[] RenderRollBackAndUnlock()
        {
            return new String[]
          {
                "ROLLBACK;",
                "UNLOCK TABLES;"
          };
        }

        public override string renderDateTime(DateTime t)
        {
            return MySQLDialectHelper.RenderDateTime(t);
        }

        public override string GetSQLVariable(string v)
        {
            return "@"+v;
        }

        public override string RenderNowToVariable(string v)
        {
            return String.Format("SET {0} = NOW(3)", this.GetSQLVariable(v));
        }

        public override string RenderTmpTableFromSelect(string tableSchema, string tableName, string select)
        {
            return String.Format("CREATE TEMPORARY TABLE IF NOT EXISTS  {0} AS ({1});",
                this.PrepareTable(new Table() { TableName = tableName, TableSchema = tableSchema }),
                select);
        }

        public override string RenderDropTempTable(Table tmpTable)
        {
            return String.Format("DROP TEMPORARY TABLE {0};", this.PrepareTable(tmpTable));
        }

        public override string[] RenderLockTablesAliased(TableToLock[] tables)
        {

            String[] tablesWithWrite = tables.Select(x => { return x.Name +  (String.IsNullOrWhiteSpace(x.Alias) ? "" : (" AS " + x.Alias)) + (x.LockAsWrite ? " WRITE" : " READ"); }).ToArray();
            String lockTables = String.Format("LOCK TABLES {0}", String.Join(",", tablesWithWrite)) + ";";
            String[] setupAndAquireLock = new String[] {
                lockTables
            };
            return setupAndAquireLock;
        }
    }
}
