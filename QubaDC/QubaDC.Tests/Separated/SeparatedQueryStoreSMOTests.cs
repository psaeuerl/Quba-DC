using QubaDC.CRUD;
using QubaDC.Restrictions;
using QubaDC.Separated;
using QubaDC.SMO;
using QubaDC.Tests.CustomAsserts;
using QubaDC.Tests.DataBuilder;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace QubaDC.Tests.Separated
{
    public class SeparatedQueryStoreSMOTests : IDisposable
    {
        private string currentDatabase;

        public SeparatedQueryStoreSMOTests()
        {
            this.fixture = new MySqlDBFixture();
            MySQLDataConnection con = fixture.DataConnection.Clone();
            QubaDCSystem c = new MySQLQubaDCSystem(
                        con,
                         new SeparatedSMOHandler()
                         ,new SeparatedCRUDHandler()
                         , new SeparatedQSSelectHandler()
                       );
            this.QBDC = c;
            //Create Empty Schema
            this.currentDatabase = "SeparatedQueryStoreTests_" + Guid.NewGuid().ToString().Replace("-", "");
            fixture.CreateEmptyDatabase(currentDatabase);
            con.UseDatabase(currentDatabase);
            this.fixture = fixture;
        }

        public MySqlDBFixture fixture { get; private set; }
        public QubaDCSystem QBDC { get; private set; }
        public bool Succcess { get; private set; } = false;

        public void Dispose()
        {
            if(Succcess)
                this.fixture.DropDatabaseIfExists(currentDatabase);
        }

        [Fact]
        public void RenameTableWorks()
        {
            //Create Basic Table
            QBDC.Init();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);
            //Insert some data
            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'asdf'");
            QBDC.CRUDHandler.HandleInsert(c);
            InsertOperation c2 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "2", "'ehji'");
            QBDC.CRUDHandler.HandleInsert(c2);
            ////Make a Request
            var schema = QBDC.SchemaManager.GetCurrentSchema();
            SelectOperation s =  SelectOperation.FromCreateTable(t);
            var result = QBDC.QueryStore.ExecuteSelect(s);

            Assert.Equal("98dec3754faa19997a14b0b27308bb63", result.Hash);

            RenameTable rt = new RenameTable()
            {
                OldSchema = t.Schema,
                OldTableName = t.TableName,
                NewSchema = t.Schema,
                NewTableName = "new_basic_table"
            };
            QBDC.SMOHandler.HandleSMO(rt);

            var newSchema = QBDC.SchemaManager.GetCurrentSchema();

            var result2 = QBDC.QueryStore.ReExecuteSelect(result.GUID);

            //check that new schema contains renamed table
            //check that new schema does not contain original table

            SchemaInfo newSchemaInfo = QBDC.SchemaManager.GetCurrentSchema();
            Assert.Equal(3, newSchemaInfo.ID);
            Assert.IsType<RenameTable>(newSchemaInfo.SMO);
            Assert.True(newSchemaInfo.Schema.ContainsTable(rt.NewSchema, rt.NewTableName));
            Assert.False(newSchemaInfo.Schema.ContainsTable(rt.OldSchema, rt.OldTableName));
            Assert.Throws<InvalidOperationException>(() => newSchemaInfo.Schema.FindTable(rt.OldSchema, rt.OldTableName));
            QueryStoreAsserts.ReexcuteIsCorrect(result, result2);
            this.Succcess = true;
        }


        [Fact]
        public void DropTableWorks()
        {
            //Create Basic Table
            QBDC.Init();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);
            //Insert some data
            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'asdf'");
            QBDC.CRUDHandler.HandleInsert(c);
            InsertOperation c2 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "2", "'ehji'");
            QBDC.CRUDHandler.HandleInsert(c2);
            ////Make a Request
            var schema = QBDC.SchemaManager.GetCurrentSchema();
            SelectOperation s = SelectOperation.FromCreateTable(t);
            var result = QBDC.QueryStore.ExecuteSelect(s);

            Assert.Equal("98dec3754faa19997a14b0b27308bb63", result.Hash);

            
            DropTable dt = new DropTable()
            {
                Schema = t.Schema,
                TableName = t.TableName,             
            };
            QBDC.SMOHandler.HandleSMO(dt);

            var newSchema = QBDC.SchemaManager.GetCurrentSchema();

            var result2 = QBDC.QueryStore.ReExecuteSelect(result.GUID);

            //check that new schema contains renamed table
            //check that new schema does not contain original table

            SchemaInfo newSchemaInfo = QBDC.SchemaManager.GetCurrentSchema();
            Assert.Equal(3, newSchemaInfo.ID);
            Assert.False(newSchemaInfo.Schema.ContainsTable(dt.Schema, dt.TableName));

            QueryStoreAsserts.ReexcuteIsCorrect(result, result2);
            this.Succcess = true;
        }


        [Fact]
        public void CopyTableWorks()
        {
            //Create Basic Table
            QBDC.Init();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);
            //Insert some data
            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'asdf'");
            QBDC.CRUDHandler.HandleInsert(c);
            InsertOperation c2 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "2", "'ehji'");
            QBDC.CRUDHandler.HandleInsert(c2);
            ////Make a Request
            var schema = QBDC.SchemaManager.GetCurrentSchema();
            SelectOperation s = SelectOperation.FromCreateTable(t);
            var result = QBDC.QueryStore.ExecuteSelect(s);

            Assert.Equal("98dec3754faa19997a14b0b27308bb63", result.Hash);


            CopyTable ct = new CopyTable()
            {
                Schema = t.Schema,
                TableName = t.TableName,
                CopiedSchema = t.Schema,
                CopiedTableName = "copied_basic_table"
            };

            QBDC.SMOHandler.HandleSMO(ct);

            var newSchema = QBDC.SchemaManager.GetCurrentSchema();

            //check that new schema contains copied table            
            //check that they have the same data

            SchemaInfo newSchemaInfo = QBDC.SchemaManager.GetCurrentSchema();
            Assert.Equal(3, newSchemaInfo.ID);
            var xy = newSchemaInfo.Schema.FindTable(ct.CopiedSchema, ct.CopiedTableName);
            Assert.True(newSchemaInfo.Schema.ContainsTable(ct.CopiedSchema, ct.CopiedTableName));

            String[] triggersOnCopeidTable = this.fixture.GetTriggersForTable(ct.CopiedSchema, ct.CopiedTableName);
            Assert.Equal(3, triggersOnCopeidTable.Length);

            //Check that they contain the same data
            SelectOperation s2 = SelectOperation.FromCreateTable(t);
            var result2 = QBDC.QueryStore.ExecuteSelect(s2);
            Assert.Equal("98dec3754faa19997a14b0b27308bb63", result2.Hash);

            s2.FromTable = new FromTable() { TableSchema = ct.CopiedSchema, TableName = ct.CopiedTableName, TableAlias="ref" };
            s2.Columns = s2.Columns.Select(x => new ColumnReference() { ColumnName = x.ColumnName, TableReference = "ref" }).ToArray();      
            var result3 = QBDC.QueryStore.ExecuteSelect(s2);
  
            Assert.Equal("98dec3754faa19997a14b0b27308bb63", result3.Hash);

            this.Succcess = true;
        }

        [Fact]
        public void PartitionTableWorks()
        {
            //Create Basic Table
            QBDC.Init();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);
            //Insert some data
            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'asdf'");
            QBDC.CRUDHandler.HandleInsert(c);
            InsertOperation c2 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "2", "'ehji'");
            QBDC.CRUDHandler.HandleInsert(c2);
            ////Make a Request
            var schema = QBDC.SchemaManager.GetCurrentSchema();
            SelectOperation s = SelectOperation.FromCreateTable(t);
            var result = QBDC.QueryStore.ExecuteSelect(s);
            Assert.Equal("98dec3754faa19997a14b0b27308bb63", result.Hash);

            PartitionTable ct = new PartitionTable()
            {
                BaseSchema = t.Schema,
                BaseTableName = t.TableName,
                FalseConditionSchema = t.Schema,
                FalseConditionTableName = "FalseTable",
                TrueConditionSchema = t.Schema,
                TrueConditionTableName = "TrueTable",
                Restriction = new OperatorRestriction()
                {
                    LHS = new ColumnOperand() { Column = new ColumnReference() { ColumnName = "ID", TableReference = t.TableName } },
                    Op = RestrictionOperator.Equals,
                    RHS = new LiteralOperand() { Literal = "1" }
                }
            };

            QBDC.SMOHandler.HandleSMO(ct);

            var newSchema = QBDC.SchemaManager.GetCurrentSchema();

            //check that new schema contains two new tables table            
            //check that they have the same data

            SchemaInfo schemainfo = QBDC.SchemaManager.GetCurrentSchema();
            Assert.Equal(3, schemainfo.ID);
            Assert.True(schemainfo.Schema.ContainsTable(ct.FalseConditionSchema, ct.FalseConditionTableName));
            Assert.True(schemainfo.Schema.ContainsTable(ct.TrueConditionSchema, ct.TrueConditionTableName));
            Assert.False(schemainfo.Schema.ContainsTable(ct.BaseSchema, ct.BaseTableName));
            //Check that they contain the right data
            SelectOperation s2 = SelectOperation.FromCreateTable(t);


            s2.FromTable = new FromTable() { TableSchema = ct.TrueConditionSchema, TableName = ct.TrueConditionTableName, TableAlias = "ref" };
            s2.Columns = s2.Columns.Select(x => new ColumnReference() { ColumnName = x.ColumnName, TableReference = "ref" }).ToArray();
            var result3 = QBDC.QueryStore.ExecuteSelect(s2);
            Assert.Equal(1, result3.Result.Rows.Count);
            Assert.Equal(1, result3.Result.Select().First()[0]);

            s2.FromTable = new FromTable() { TableSchema = ct.FalseConditionSchema, TableName = ct.FalseConditionTableName, TableAlias = "ref" };
            s2.Columns = s2.Columns.Select(x => new ColumnReference() { ColumnName = x.ColumnName, TableReference = "ref" }).ToArray();
            var result4 = QBDC.QueryStore.ExecuteSelect(s2);
            Assert.Equal(1, result4.Result.Rows.Count);
            Assert.Equal(2, result4.Result.Select().First()[0]);
            this.Succcess = true;
        }
       
        [Fact]
        public void MergeTableWorks()
        {
            //Create Basic Table
            QBDC.Init();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);

            CreateTable t2 = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            t2.TableName = "basictable2";
            QBDC.SMOHandler.HandleSMO(t2);
            //Insert some data
            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'b1table'");
            QBDC.CRUDHandler.HandleInsert(c);


            InsertOperation c2 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "2", "'b2table'");
            c2.InsertTable.TableName = t2.TableName;
            QBDC.CRUDHandler.HandleInsert(c2);

            ////Make a Request
            var schema = QBDC.SchemaManager.GetCurrentSchema();
            //SelectOperation s = SelectOperation.FromCreateTable(t);
            //var result = QBDC.QueryStore.ExecuteSelect(s);

            //Assert.Equal("98dec3754faa19997a14b0b27308bb63", result.Hash);

            MergeTable mt = new MergeTable()
            {
                ResultSchema = t.Schema,
                ResultTableName = "mergedtable",
                FirstSchema = t.Schema,
                FirstTableName = t.TableName,
                SecondSchema = t2.Schema,
                SecondTableName = t2.TableName
            };

            QBDC.SMOHandler.HandleSMO(mt);

            var newSchema = QBDC.SchemaManager.GetCurrentSchema();

            ////check that new schema contains copied table            
            ////check that they have the same data

            SchemaInfo newSchemaInfo = QBDC.SchemaManager.GetCurrentSchema();
            Assert.Equal(4, newSchemaInfo.ID);
            Assert.Equal(1, newSchemaInfo.Schema.Tables.Count());
            var xy = newSchemaInfo.Schema.FindTable(mt.ResultSchema, mt.ResultTableName);
            Assert.True(newSchemaInfo.Schema.ContainsTable(mt.ResultSchema, mt.ResultTableName));

           String[] triggersOnCopeidTable = this.fixture.GetTriggersForTable(mt.ResultSchema, mt.ResultTableName);
           Assert.Equal(3, triggersOnCopeidTable.Length);

            ////Check that they contain the same data
            SelectOperation s2 = SelectOperation.FromCreateTable(t);
            //var result2 = QBDC.QueryStore.ExecuteSelect(s2);
            //Assert.Equal("98dec3754faa19997a14b0b27308bb63", result2.Hash);

            s2.FromTable = new FromTable() { TableSchema = mt.ResultSchema, TableName = mt.ResultTableName, TableAlias = "ref" };
            s2.Columns = s2.Columns.Select(x => new ColumnReference() { ColumnName = x.ColumnName, TableReference = "ref" }).ToArray();
            var selectFromMergedTable = QBDC.CRUDHandler.RenderSelectOperation(s2);
            var resulttable = QBDC.DataConnection.ExecuteQuery(selectFromMergedTable);
            Assert.Equal(2, resulttable.Rows.Count);
            this.Succcess = true;
        }

        [Fact]
        public void DecomposeTableWorks()
        {
            //Create Basic Table
            QBDC.Init();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);
            //Insert some data
            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'schema1'");
            QBDC.CRUDHandler.HandleInsert(c);


            InsertOperation c2 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "2", "'schema2'");
            c2.InsertTable.TableName = t.TableName;
            QBDC.CRUDHandler.HandleInsert(c2);



            CreateTable t2 = CreateTableBuilder.BuildBasicTablForJoin(this.currentDatabase);            
            QBDC.SMOHandler.HandleSMO(t2);

            InsertOperation t2i = CreateTableBuilder.GetBasicTableForJoinInsert(this.currentDatabase, "1", "'schema1'");
            QBDC.CRUDHandler.HandleInsert(t2i);

            InsertOperation t2j = CreateTableBuilder.GetBasicTableForJoinInsert(this.currentDatabase, "3", "'somevalue'");
            QBDC.CRUDHandler.HandleInsert(t2j);

            ////Make a Request
            var schema = QBDC.SchemaManager.GetCurrentSchema();


            JoinTable mt = new JoinTable()
            {
                ResultSchema = t.Schema,
                ResultTableName = "mergedtable",
                FirstSchema = t.Schema,
                FirstTableName = t.TableName,
                FirstTableAlias = "t1",
                SecondSchema = t2.Schema,
                SecondTableName = t2.TableName,
                SecondTableAlias = "t2",
                JoinRestriction = new AndRestriction()
                {
                    Restrictions = new Restriction[]
                      {
                           new OperatorRestriction()
                           {
                                LHS = new LiteralOperand() { Literal ="t1.ID" },
                                Op = RestrictionOperator.Equals,
                                RHS = new LiteralOperand() { Literal = "t2.ID" },
                           }
                      }
                }
            };

            QBDC.SMOHandler.HandleSMO(mt);

            var newSchema = QBDC.SchemaManager.GetCurrentSchema();

            //////check that new schema contains copied table            
            //////check that they have the same data

            SchemaInfo newSchemaInfo = QBDC.SchemaManager.GetCurrentSchema();
            Assert.Equal(4, newSchemaInfo.ID);
            Assert.Equal(1, newSchemaInfo.Schema.Tables.Count());
            var xy = newSchemaInfo.Schema.FindTable(mt.ResultSchema, mt.ResultTableName);
            Assert.True(newSchemaInfo.Schema.ContainsTable(mt.ResultSchema, mt.ResultTableName));
            Assert.False(newSchemaInfo.Schema.ContainsTable(mt.FirstSchema, mt.FirstTableName));
            Assert.False(newSchemaInfo.Schema.ContainsTable(mt.SecondSchema, mt.SecondTableName));

            String[] triggersOnCopeidTable = this.fixture.GetTriggersForTable(mt.ResultSchema, mt.ResultTableName);
            Assert.Equal(3, triggersOnCopeidTable.Length);

            //////Check that they contain the same data
            SelectOperation s2 = SelectOperation.FromCreateTable(t);
            s2.Columns = t.Columns.Union(t2.Columns).Distinct().Select(x => new ColumnReference() { ColumnName = x.ColumName, TableReference = s2.FromTable.TableAlias }).ToArray();
            s2.FromTable.TableName = mt.ResultTableName;
            var result2 = QBDC.QueryStore.ExecuteSelect(s2);
            Assert.Equal(1, result2.Result.Rows.Count);
            this.Succcess = true;
        }

        [Fact]
        public void DropColumnWorks()
        {
            //Create Basic Table
            QBDC.Init();
            CreateTable t = CreateTableBuilder.BuildBasicTable(this.currentDatabase);
            QBDC.SMOHandler.HandleSMO(t);
            //Insert some data
            InsertOperation c = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "1", "'schema1'");
            QBDC.CRUDHandler.HandleInsert(c);


            InsertOperation c2 = CreateTableBuilder.GetBasicTableInsert(this.currentDatabase, "2", "'schema2'");
            c2.InsertTable.TableName = t.TableName;
            QBDC.CRUDHandler.HandleInsert(c2);

            ////Make a Request
            var schema = QBDC.SchemaManager.GetCurrentSchema();


            DropColumn mt = new DropColumn()
            {
                Schema = t.Schema,
                TableName = t.TableName,
                Column = "Info"
            };
               

            QBDC.SMOHandler.HandleSMO(mt);

            var newSchema = QBDC.SchemaManager.GetCurrentSchema();

            //////check that new schema contains copied table            
            //////check that they have the same data

            SchemaInfo newSchemaInfo = QBDC.SchemaManager.GetCurrentSchema();
            Assert.Equal(3, newSchemaInfo.ID);
            Assert.Equal(1, newSchemaInfo.Schema.Tables.Count());
            Assert.Equal(2, newSchemaInfo.Schema.Tables.First().Table.Columns.Count());
            

           

            ////////Check that they contain the same data
            SelectOperation s2 = SelectOperation.FromCreateTable(t);
            s2.Columns = t.Columns.Where(x=>x.ColumName!=mt.Column).Select(x => new ColumnReference() { ColumnName = x.ColumName, TableReference = s2.FromTable.TableAlias }).ToArray();
            //s2.FromTable.TableName = mt.ResultTableName;
            var result2 = QBDC.QueryStore.ExecuteSelect(s2);
            Assert.Equal(2, result2.Result.Rows.Count);
            this.Succcess = true;
        }
    }
}
