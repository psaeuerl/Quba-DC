using QubaDC.CRUD;
using QubaDC.Restrictions;
using QubaDC.SMO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Tests.DataBuilder
{
    public class CreateTableBuilder
    {
        public static CreateTable BuildBasicTable(String schema)
        {
            return BuildBasicTable(schema, "bisctable");
            
        }

        public static CreateTable BuildBasicTable(String schema,String name)
        {
            return new CreateTable()
            {
                TableName = name,
                Schema = schema,
                Columns = new ColumnDefinition[] {
                    new ColumnDefinition() {  ColumName = "ID",  DataType =" INT", Nullable = false },
                    new ColumnDefinition() {  ColumName = "Schema",  DataType =" MediumText", Nullable = false }
                },
                PrimaryKey = new String[] { "ID" }
            };
        }

        public static InsertOperation GetBasicTableInsert(string currentdatabase, string id, string value)
        {
            CreateTable t = CreateTableBuilder.BuildBasicTable(currentdatabase);
            return new InsertOperation()
            {
                ColumnNames = t.GetColumnNames(),
                InsertTable = t.ToTable(),
                ValueLiterals = new String[] {id, value}
            };
        }

        public static DeleteOperation GetBasicTableDelete(string currentdatabase, string id, string schema)
        {
            CreateTable t = CreateTableBuilder.BuildBasicTable(currentdatabase);
            return new DeleteOperation()
            {

                Table = t.ToTable(),
                Restriction = new AndRestriction()
                {
                    Restrictions = new Restriction[]
                     {
                          new OperatorRestriction()
                          {
                                LHS = new ColumnOperand() { Column = new ColumnReference() { ColumnName = "ID", TableReference ="bisctable" } },
                                 Op = RestrictionOperator.Equals,
                                 RHS = new LiteralOperand() { Literal = id }
                          },
                                                    new OperatorRestriction()
                          {
                                LHS = new ColumnOperand() { Column = new ColumnReference() { ColumnName = "Schema", TableReference ="bisctable" } },
                                 Op = RestrictionOperator.Equals,
                                 RHS = new LiteralOperand() { Literal = schema }
                          }
                     }
                }
            };

        }
    }
}
