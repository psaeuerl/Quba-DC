using QubaDC.CRUD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Separated.CRUD
{
    class SeparatedSelectHandler
    {
        public SeparatedSelectHandler(DataConnection c, SchemaManager schemaManager, CRUDRenderer crudRender)
        {
            this.DataConnection = c;
            this.SchemaManager = schemaManager;
            this.CRUDRenderer = crudRender;
        }

        public CRUDRenderer CRUDRenderer { get; private set; }
        public DataConnection DataConnection { get; private set; }
        public SchemaManager SchemaManager { get; private set; }

        internal void HandleSelect(SelectOperation selectOperation)
        {
            //    //What happens here?
            //    //a.) Reformulate Query
            //    TargetTable[] targetTables = GenerateTargetTables(selectOperation);
            //    SchemaInfo[] schemas = SchemaManager.GetAllSchemataOrderdByIdDescending();
            //    foreach (var schemaInfo in schemas)
            //    {
            //        ApplySchemaChangesToTargetTables(schemaInfo, targetTables);
            //    }
            //    String query = RenderSelect(selectOperation, targetTables);
            //    //b.) Execute Query

            //    //c.) Calculate the value and all
            //    //d.) Store in QS



            //What happes in the Insert Operation?

            //a => get the current schema
            //b => build insert statement for the history table
            ////Insert both
            //this.DataConnection.DoTransaction((trans, con) =>
            //{
            //    String insertIntoBaseTable = this.CRUDRenderer.RenderInsert(insertOperation.InsertTable, insertOperation.ColumnNames, insertOperation.ValueLiterals);

            //    SchemaInfo s = this.SchemaManager.GetCurrentSchema(con);
            //    Table histTable = s.Schema.FindHistTable(insertOperation.InsertTable);

            //    List<String> histcolumns = new List<string>();
            //    histcolumns.AddRange(insertOperation.ColumnNames);
            //    histcolumns.AddRange(SeparatedConstants.GetHistoryTableColumns().Select(x => x.ColumName));
            //    String[] histColumnsFinal = histcolumns.ToArray();

            //    List<String> histValues = new List<string>();
            //    histValues.AddRange(insertOperation.ValueLiterals);
            //    histValues.AddRange(new String[] {
            //    CRUDRenderer.SerializeDateTime(DateTime.Now),
            //       "NULL",
            //       CRUDRenderer.SerializeString(Guid.NewGuid().ToString())
            //    });
            //    String[] histValuesFinal = histValues.ToArray();

            //    String insertIntoHistTable = this.CRUDRenderer.RenderInsert(histTable, histColumnsFinal, histValuesFinal);
            //    this.DataConnection.ExecuteQuery(insertIntoBaseTable, con);
            //    this.DataConnection.ExecuteQuery(insertIntoHistTable, con);
            //    trans.Commit();
            //});
        }

        //private string RenderSelect(SelectOperation selectOperation, TargetTable[] targetTables)
        //{
        //    throw new NotImplementedException();
        //}

        //private void ApplySchemaChangesToTargetTables(SchemaInfo schemaInfo, TargetTable[] targetTables)
        //{
        //    throw new NotImplementedException();
        //}

        //private TargetTable[] GenerateTargetTables(SelectOperation selectOperation)
        //{
        //    var SelectTables = selectOperation.GetAllSelectedTables();
        //    //Assign Columns
        //    var tableWithColumns = SelectTables.Select(x =>
        //    new
        //    {
        //        Columns = selectOperation.Columns.Where(y => y.TableReference == x.TableAlias).ToArray(),
        //        Table = x
        //    });
        //    TargetTable[] result = tableWithColumns.Select(x => TargetTable.FromSelectTableWithColumns(x.Columns, x.Table)).ToArray();
        //    return result;
        //}
    }
}