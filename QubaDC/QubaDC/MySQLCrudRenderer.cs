using System;
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

        internal String RenderColumn(ColumnReference column)
        {
            return QualifyObjectName(column.TableReference) + "." + QualifyObjectName(column.ColumnName);
        }

        private string PrepareColumn(string[] columnNames)
        {
            var cols = columnNames.Select(x =>  QualifyObjectName(x.ToLowerInvariant()));
            return String.Join("," + System.Environment.NewLine, cols);
        }

        private string PrepareTable(Table insertTable)
        {
            return QualifyObjectName(insertTable.TableSchema.ToLowerInvariant())
                +"."
                + QualifyObjectName(insertTable.TableName.ToLowerInvariant());
        }

        public override string SerializeDateTime(DateTime now)
        {
            return MySQLDialectHelper.RenderDateTime(now);
            //String format = "{{ ts '{0:D4}-{1:D2}-{2:D2} {3:D2}:{4:D2}:{5:D2}.{6:D}' }}";
            //String result = String.Format(format, now.Year, now.Month, now.Day,now.Hour,now.Minute,now.Second,now.Millisecond%1000000);
            //return result;
        }

        internal override string SerializeString(string v)
        {
            return "'" + v + "'";
        }

        internal override string RenderRestriction(Restriction joinCondition)
        {
            if (joinCondition == null)
                return "";
            MySQLRestrictionVisitor v = new MySQLRestrictionVisitor(this);
            String result = joinCondition.Accept<String>(v);
            return result;
        }

        internal override string RenderDelete(Table table, Restriction restriction)
        {
            String restrictions = RenderRestriction(restriction);
            String restPart = String.IsNullOrWhiteSpace(restrictions) ? "" : "WHERE " + restrictions;
            String tableRef = PrepareTable(table);

            String baseDelete = "DELETE FROM {0} {1}";
            String deleteResult = String.Format(baseDelete, tableRef, restPart);
            return deleteResult;
        }
    }
}
