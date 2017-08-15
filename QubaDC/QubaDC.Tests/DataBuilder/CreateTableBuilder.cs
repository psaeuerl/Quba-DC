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
        public const String BasicTableName = "basictable";
        public const String BasicTableForJoinName = "jointable";
        public static CreateTable BuildBasicTable(String schema)
        {
            return BuildBasicTable(schema, BasicTableName);
            
        }
        public static CreateTable BuildBasicTablForJoin(String schema)
        {
            return BuildBasicTablForJoin(schema, BasicTableForJoinName);

        }

        public static CreateTable BuildBasicTable(String schema,String name)
        {
            return new CreateTable()
            {
                TableName = name,
                Schema = schema,
                Columns = new ColumnDefinition[] {
                    new ColumnDefinition() {  ColumName = "ID",  DataType =" INT", Nullable = false },
                    new ColumnDefinition() {  ColumName = "Schema",  DataType =" MediumText", Nullable = false },
                     new ColumnDefinition() {  ColumName = "Info",  DataType =" MediumText", Nullable = true }

                },
                PrimaryKey = new String[] { }//new String[] { "ID" }
            };
        }

        public static CreateTable BuildBasicTablForJoin(String schema, String name)
        {
            return new CreateTable()
            {
                TableName = name,
                Schema = schema,
                Columns = new ColumnDefinition[] {
                    new ColumnDefinition() {  ColumName = "ID",  DataType =" INT", Nullable = false },
                    new ColumnDefinition() {  ColumName = "Something",  DataType =" MediumText", Nullable = false },
                },
                PrimaryKey = new String[] { }//new String[] { "ID" }
            };
        }

        public static InsertOperation GetBasicTableInsert(string currentdatabase, string id, string value, string info = "null")
        {
            CreateTable t = CreateTableBuilder.BuildBasicTable(currentdatabase);
            return new InsertOperation()
            {
                ColumnNames = t.GetColumnNames(),
                InsertTable = t.ToTable(),
                ValueLiterals = new String[] {id, value, info}
            };
        }

        public static InsertOperation GetBasicTableForJoinInsert(string currentdatabase, string id, string something)
        {
            CreateTable t = CreateTableBuilder.BuildBasicTablForJoin(currentdatabase);
            return new InsertOperation()
            {
                ColumnNames = t.GetColumnNames(),
                InsertTable = t.ToTable(),
                ValueLiterals = new String[] { id, something }
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
                                LHS = new ColumnOperand() { Column = new ColumnReference() { ColumnName = "ID", TableReference =BasicTableName } },
                                 Op = RestrictionOperator.Equals,
                                 RHS = new LiteralOperand() { Literal = id }
                          },
                                                    new OperatorRestriction()
                          {
                                LHS = new ColumnOperand() { Column = new ColumnReference() { ColumnName = "Schema", TableReference =BasicTableName } },
                                 Op = RestrictionOperator.Equals,
                                 RHS = new LiteralOperand() { Literal = schema }
                          }
                     }
                }
            };

        }

        public static UpdateOperation GetBasicTableUpdate(string currentdatabase, string oldid, string newvalue)
        {
            CreateTable t = CreateTableBuilder.BuildBasicTable(currentdatabase);
            return new UpdateOperation()
            {

                Table = t.ToTable(),
                Restriction = new AndRestriction()
                {
                    Restrictions = new Restriction[]
                     {
                          new OperatorRestriction()
                          {
                                LHS = new ColumnOperand() { Column = new ColumnReference() { ColumnName = "ID", TableReference =BasicTableName } },
                                 Op = RestrictionOperator.Equals,
                                 RHS = new LiteralOperand() { Literal = oldid }
                          }
                     }
                },
                ColumnNames = new string[] { "Schema" },
                 ValueLiterals = new string[] {"'"+newvalue+"'"}
            };
        }
    }
}
